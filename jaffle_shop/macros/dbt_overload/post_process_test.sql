{%- macro post_process_test(model, identifier, schema, database, comm) %}
    {{ adapter.dispatch('post_process_test')(model, identifier, schema, database, comm) }}
{%- endmacro %}

{%- macro default__post_process_test(model, identifier, schema, database, comm) %}
{%- endmacro %}

{%- macro sqlserver__post_process_test(model, identifier, schema, database, comm) %}
    
    {% set path = model.get("original_file_path") %}
    {% set filename = path.split('/')[-1] %}
    {% set name = filename.split('.')[0] %}
    {% set model_name = name.split('_', 1)[1] %}
    {% set model_path = path.rsplit('/', 1)[0] %}
    {% set column_name = model.get("column_name") or "Not a column" %}
    {% set tags = model.get("tags")|string %}
    {% set tags = tags|replace("'", '"') %}
    {% set severity = model.get('unrendered_config', {}).get('severity') or "INFO" %}
    
    {% set table_name = "MONITOR_TABLE" %}
    {% set backup_table = table_name ~ '_OLD_' ~ modules.datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') %}

    {% set expected_columns %}
    id_run INT IDENTITY(12485,1)
    , num_rows INT
    , dat_execution DATETIME
    , nam_model_path VARCHAR(64)
    , nam_column VARCHAR(64)
    , nam_test VARCHAR(64)
    , nam_model VARCHAR(64)
    , nam_tags VARCHAR(64)
    , nam_severity VARCHAR(64)
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
        {% set expected_sig = "id_run,num_rows,dat_execution,nam_model_path,nam_column,nam_test,nam_model,nam_tags,nam_severity" %}

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

    {# 3 - Insert Monitoring data#}
    {% set insert_query %}
    WITH test_exec AS (
        SELECT 
            COUNT(*) AS num_rows,
            CURRENT_TIMESTAMP AS dat_execution,
            '{{ model_path }}' AS nam_model_path,
            '{{ column_name }}' AS nam_column,
            '{{ identifier }}' AS nam_test,
            '{{ model_name }}' AS nam_model,
            '{{ tags }}' AS nam_tags,
            '{{ severity }}' AS nam_severity 
        FROM {{ database }}.{{ schema }}.{{ identifier }}
    ) 
    INSERT INTO {{ database }}.{{ schema }}.{{ table_name }} (
        num_rows, 
        dat_execution, 
        nam_model_path, 
        nam_column, 
        nam_test, 
        nam_model,
        nam_tags,
        nam_severity
    )    
    SELECT  
        SUM(num_rows),
        dat_execution,
        nam_model_path,
        nam_column,
        nam_test,
        nam_model,
        nam_tags,
        nam_severity
    FROM test_exec 
    GROUP BY 
        dat_execution, 
        nam_model_path, 
        nam_column, 
        nam_test, 
        nam_model,
        nam_tags,
        nam_severity
    {% endset %}

    {% do run_query(insert_query) %}
    
    {# 4 - Call macro to store anomalies #}
    {% do histo_anomalie(model, database, schema, identifier) %}

{%- endmacro %}