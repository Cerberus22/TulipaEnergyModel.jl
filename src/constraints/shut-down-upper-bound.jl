export add_shut_down_upper_bound_constraints!

"""
    add_shut_down_upper_bound_constraints!(model, constraints)

Adds the shut down constraints to the model.
"""
function add_shut_down_upper_bound_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
)
    let table_name = :shut_down_upper_bound, cons = constraints[:shut_down_upper_bound]
        expr_avail_simple_method =
            expressions[:available_asset_units_simple_method].expressions[:assets]

        indices =
            _append_available_units_shut_down_simple_method(connection, :shut_down_upper_bound)

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                @constraint(
                    model,
                    shut_down <= expr_avail_simple_method[row.avail_id] - units_on,
                    base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, shut_down, units_on) in
                zip(indices, variables[:shut_down].container, variables[:units_on].container)
            ],
        )
    end
    println()
    return nothing
end

function _append_available_units_shut_down_simple_method(connection, table_name)
    return DuckDB.query(
        connection,
        "SELECT
            cons.*,
            expr_avail.id AS avail_id,
        FROM cons_$table_name AS cons
        LEFT JOIN expr_available_asset_units_simple_method AS expr_avail
            ON cons.asset = expr_avail.asset
            AND cons.year = expr_avail.milestone_year
        LEFT JOIN asset
            ON cons.asset = asset.asset
        WHERE asset.investment_method in ('simple', 'none')
        ORDER BY cons.id
        ",
    )
end
