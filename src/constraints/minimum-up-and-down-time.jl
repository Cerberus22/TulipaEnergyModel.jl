export add_minimum_up_time_constraints!

"""
    add_minimum_up_time_constraints!(model, constraints)

Adds the minimum up time constraints to the model.
"""
function add_minimum_up_time_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
)
    let table_name = :minimum_up_time, cons = constraints[:minimum_up_time]
        asset_year_rep_period_dict = Dict()
        for row in cons.indices
            key = "$(row.asset),$(row.year),$(row.rep_period)"
            if (!haskey(asset_year_rep_period_dict, key))
                asset_year_rep_period_dict[key] = _get_and_append_min_up_down_time_data_to_indices(connection, table_name, row.asset, row.year, row.rep_period)
            end
        end
        attach_constraint!(
            model,
            cons,
            table_name,
            [
                @constraint(
                    model,
                    _sum_min_up_blocks(asset_year_rep_period_dict["$(row.asset),$(row.year),$(row.rep_period)"], variables[:start_up].container, row.time_block_start) <= units_on,
                    base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, start_up, units_on) in
                zip(cons.indices, variables[:start_up].container, variables[:units_on].container)
            ],
        )
    end
end

function _sum_min_up_blocks(sum_rows, start_ups, start_of_curr_constraint)
    sum = 0
    for single_row in sum_rows
        start_of_this = single_row.time_block_start
        minimum_up_time = single_row.minimum_up_time
        if (start_of_curr_constraint - minimum_up_time + 1 <= start_of_this <= start_of_curr_constraint)
            sum = sum + start_ups[single_row.id]
        end
    end
    return sum
end

function _get_and_append_min_up_down_time_data_to_indices(connection, table_name, curr_asset, curr_year, curr_rep_period)
    return DuckDB.query(
        connection,
        "SELECT
            cons.*,
            asset.minimum_up_time,
            asset.minimum_down_time
        FROM cons_$table_name AS cons
        LEFT JOIN asset
            ON cons.asset = asset.asset
        WHERE cons.asset = '$curr_asset' AND cons.year = $curr_year AND cons.rep_period = $curr_rep_period
        ORDER BY cons.id
        ",
    )
end
