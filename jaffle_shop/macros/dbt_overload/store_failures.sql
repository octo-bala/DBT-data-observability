{% macro histo_anomalie(model, database, schema, identifier) %}
    {# 1. Create table HISTO_FAILURES_TABLE if not exists #}

    {% set table_name = "HISTO_FAILURES_TABLE" %}
    {% set backup_table = table_name ~ '_OLD_' ~ modules.datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') %}

    {% set expected_columns %}
        id_anomaly INT IDENTITY(1,1) PRIMARY KEY,
        id_run varchar(255),
        id_context varchar(max),
        anomaly_column_value varchar(64)
    {% endset %}

    {# 1 - Check if table exists #}
    {% set table_exists_query %}
        SELECT 1 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{{ table_name }}'
        AND TABLE_SCHEMA = '{{ schema }}'
        AND TABLE_CATALOG = '{{ database }}'
    {% endset %}

    {% set table_exists_result = run_query(table_exists_query) %}
    {% set table_exists = table_exists_result.rows|length > 0 %}

    {% if not table_exists %}
        {% set create_query %}
        CREATE TABLE {{ database }}.{{ schema }}.{{ table_name }} (
            {{ expected_columns }}
        );
        {% endset %}
        {% do run_query(create_query) %}
    {% else %}
        {# 2 - Table exists: check if structure is different #}
        {% set diff_query %}
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{{ table_name }}'
        AND TABLE_SCHEMA = '{{ schema }}'
        AND TABLE_CATALOG = '{{ database }}'
        ORDER BY ORDINAL_POSITION
        {% endset %}

        {% set existing_columns = run_query(diff_query).rows %}
        {% set existing_sig = existing_columns | map(attribute=0) | join(',') %}
        {% set expected_sig = "id_anomaly,id_run,id_context,anomaly_column_value" %}

        {% if existing_sig != expected_sig %}
            {# Save the old version of the table #}
            
            {% set rename_query %}
            EXEC sp_rename '{{ schema }}.{{ table_name }}', '{{ backup_table }}';
            {% endset %}
            {% do run_query(rename_query) %}
            
            {% set create_new_query %}
            CREATE TABLE {{ database }}.{{ schema }}.{{ table_name }} (
                {{ expected_columns }}
            );
            {% endset %}
            {% do run_query(create_new_query) %}
            
        {% endif %}
    {% endif %}

















{# 2.Retrieving the test ID from MONITOR TABLE #}
    {% set id_query = " select top 1 id_run from  "+database +"."+ schema +".MONITOR_TABLE
                        where nam_test = '"+identifier+"' order by dat_execution desc " %}
    {% set id_res = run_query(id_query) %}

    {% if id_res.rows | length == 0 %}
        {% do exceptions.raise_compiler_error("No IDs found in MONITOR_TABLE.") %}
    {% endif %}
    {% set id_run_monitor = id_res.rows[0].values()[0] %}

{# 3. Retrieving the column names in error and table names #}
    {% set meta =model.get('unrendered_config', {}).get('meta') or [] %}
    {% set base_schema = schema.split('_dbt_test__audit') %}
    {% set ref_nom_table = model.refs %}
    {% set nom_table = ref_nom_table[0].name %}
    {% set tested_column = model.get("column_name") or "Not a column" %}
    {% set test_condition =model.get('unrendered_config', {}).get('where') or "NotGiven" %}
    {% set nam_column_list = [] %}

    {% if identifier.startswith("unique") %}
        {% set nam_column = 'unique_field' %}
    {% elif identifier.startswith("accepted_values") %}
        {% set nam_column = 'value_field' %}
    {% elif identifier.startswith("relationships") %}
        {% set nam_column = 'from_field' %}
        {% set nom_table = ref_nom_table[1].name %}
    {% else %}
        {% set nam_column = model.get("column_name") or "Not a column" %}  
    {% endif %}

{# 4. Build the list of columns for concatenation #}
{% set concat_expr = [] %}
{% if meta.id and meta.id | length > 0 %}
  {% for col_name in meta.id %}
    {% do concat_expr.append("COALESCE(CAST(" ~ col_name ~ " AS VARCHAR(256)), 'NULL')") %}
  {% endfor %}
{% else %}
  {% set columns_info_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                               WHERE TABLE_SCHEMA = '" ~ base_schema[0] ~ "' 
                                 AND TABLE_NAME = '" ~ nom_table ~ "' 
                               ORDER BY ORDINAL_POSITION" %}
  {% set columns_result = run_query(columns_info_query) %}
  {% for col in columns_result.rows %}
    {% do concat_expr.append("COALESCE(CAST(" ~ col[0] ~ " AS VARCHAR(256)), 'NULL')") %}
  {% endfor %}
{% endif %}

{% if concat_expr | length == 0 %}
    {% set id_context_expr = "NULL" %}
{% elif concat_expr | length == 1 %}
    {% set id_context_expr = concat_expr[0] %}
{% else %}
    {% set id_context_expr = "CONCAT_WS('|', " ~ (concat_expr | join(', ')) ~ ")" %}
{% endif %}

{# {{ log(id_context_expr, info=True) }} #}

{# 5. Insert into HISTO_FAILURES_TABLE #}
{% set bulk_insert %}
    WITH failed_values AS (
        SELECT DISTINCT {{ nam_column }} AS error_value
        FROM {{ database }}.{{ schema }}.{{ identifier }}
    )
    INSERT INTO {{ database }}.{{ schema }}.HISTO_FAILURES_TABLE 
    (id_run, id_context, anomaly_column_value)
    SELECT 
    '{{ id_run_monitor|string }}',
    {{ id_context_expr }},
    COALESCE(CAST(t.{{ tested_column }} AS VARCHAR(64)), 'NULL')
    FROM {{ database }}.{{ base_schema[0] }}.{{ nom_table }} t
    INNER JOIN failed_values fv 
    ON (t.{{ tested_column }} = fv.error_value 
        OR (t.{{ tested_column }} IS NULL AND fv.error_value IS NULL))
    {% if test_condition != 'NotGiven' %}
        WHERE {{ test_condition }}
    {% endif %}
{% endset %}


{% do run_query(bulk_insert) %}

{% endmacro %}