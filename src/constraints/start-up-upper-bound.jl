export add_start_up_upper_bound_constraints!

"""
    add_start_up_upper_bound_constraints!(model, constraints)

Adds the start up constraints to the model.
"""
function add_start_up_upper_bound_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
)
    let table_name = :start_up_upper_bound,
        cons = constraints[:start_up_upper_bound],
        start_up_vars = variables[:start_up].container,
        units_on_vars = variables[:units_on].container

        indices = _append_units_on_and_start_up_variable_ids(connection, table_name)

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                @constraint(
                    model,
                    start_up_vars[row.start_up_id] <= units_on_vars[row.units_on_id],
                    base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for row in indices
            ],
        )
    end
end

function _append_units_on_and_start_up_variable_ids(connection, table_name)
    return DuckDB.query(
        connection,
        "SELECT
            cons.*,
            var_units_on.id as units_on_id,
            var_start_up.id as start_up_id
        FROM cons_$table_name AS cons
        LEFT JOIN asset
            ON cons.asset = asset.asset
        LEFT JOIN var_units_on
            ON var_units_on.asset = cons.asset
            AND var_units_on.year = cons.year
            AND var_units_on.rep_period = cons.rep_period
            AND var_units_on.time_block_start = cons.time_block_start
        LEFT JOIN var_start_up
            ON var_start_up.asset = cons.asset
            AND var_start_up.year = cons.year
            AND var_start_up.rep_period = cons.rep_period
            AND var_start_up.time_block_start = cons.time_block_start
        ORDER BY cons.id
        ",
    )
end
