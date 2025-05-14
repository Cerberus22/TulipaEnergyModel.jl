export add_minimum_up_time_constraints!, add_minimum_down_time_constraints!

"""
    add_minimum_up_time_constraints!(model, constraints)

Adds the minimum up time constraints to the model.
"""
function add_minimum_up_time_constraints!(connection, model, variables, expressions, constraints)
    let table_name = :minimum_up_time, cons = constraints[:minimum_up_time]
        asset_year_rep_period_dict = Dict()
        for row in cons.indices
            key = "$(row.asset),$(row.year),$(row.rep_period)"
            if (!haskey(asset_year_rep_period_dict, key))
                asset_year_rep_period_dict[key] = _get_indices_for_sum(
                    connection,
                    table_name,
                    row.asset,
                    row.year,
                    row.rep_period,
                )
            end
        end
        attach_constraint!(
            model,
            cons,
            table_name,
            [
                @constraint(
                    model,
                    _sum_min_up_blocks(
                        asset_year_rep_period_dict["$(row.asset),$(row.year),$(row.rep_period)"],
                        variables[:start_up].container,
                        row.time_block_start,
                    ) <= units_on,
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
        if (
            start_of_curr_constraint - minimum_up_time + 1 <=
            start_of_this <=
            start_of_curr_constraint
        )
            sum = sum + start_ups[single_row.id]
        end
    end
    return sum
end

function _get_indices_for_sum(connection, table_name, curr_asset, curr_year, curr_rep_period)
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

"""
    add_minimum_down_time_constraints!(model, constraints)

Adds the minimum down time constraints to the model.
"""
function add_minimum_down_time_constraints!(connection, model, variables, expressions, constraints)
    #Minimum down time with simple investment strategy
    let table_name = :minimum_down_time_simple_investment,
        cons = constraints[:minimum_down_time_simple_investment]

        asset_year_rep_period_dict = Dict()
        for row in cons.indices
            key = "$(row.asset),$(row.year),$(row.rep_period)"
            if (!haskey(asset_year_rep_period_dict, key))
                asset_year_rep_period_dict[key] = _get_indices_for_sum(
                    connection,
                    table_name,
                    row.asset,
                    row.year,
                    row.rep_period,
                )
            end
        end

        expr_avail_simple_method =
            expressions[:available_asset_units_simple_method].expressions[:assets]

        indices = _append_available_units_data_simple(connection, table_name)

        attach_constraint!(
            model,
            cons,
            :minimum_down_time_simple_investment,
            [
                @constraint(
                    model,
                    _sum_min_down_blocks(
                        asset_year_rep_period_dict["$(row.asset),$(row.year),$(row.rep_period)"],
                        variables[:shut_down].container,
                        row.time_block_start,
                    ) <= expr_avail_simple_method[row.avail_id] - units_on,
                    base_name = "minimum_down_time_simple_investment[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, shut_down, units_on) in
                zip(indices, variables[:shut_down].container, variables[:units_on].container)
            ],
        )
    end

    #Minimum down time with compact investment strategy
    let table_name = :minimum_down_time_compact_investment,
        cons = constraints[:minimum_down_time_compact_investment]

        asset_year_rep_period_dict = Dict()
        for row in cons.indices
            key = "$(row.asset),$(row.year),$(row.rep_period)"
            if (!haskey(asset_year_rep_period_dict, key))
                asset_year_rep_period_dict[key] = _get_indices_for_sum(
                    connection,
                    table_name,
                    row.asset,
                    row.year,
                    row.rep_period,
                )
            end
        end

        expr_avail_compact_method =
            expressions[:available_asset_units_compact_method].expressions[:assets]

        indices = _append_available_units_data_compact(connection, table_name)

        attach_constraint!(
            model,
            cons,
            :minimum_down_time_compact_investment,
            [
                @constraint(
                    model,
                    _sum_min_down_blocks(
                        asset_year_rep_period_dict["$(row.asset),$(row.year),$(row.rep_period)"],
                        variables[:shut_down].container,
                        row.time_block_start,
                    ) <=
                    sum(expr_avail_compact_method[avail_id] for avail_id in row.avail_indices) -
                    units_on,
                    base_name = "minimum_down_time_compact_investment[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, shut_down, units_on) in
                zip(indices, variables[:shut_down].container, variables[:units_on].container)
            ],
        )
    end
end

function _sum_min_down_blocks(sum_rows, shut_downs, start_of_curr_constraint)
    sum = 0
    for single_row in sum_rows
        start_of_this = single_row.time_block_start
        minimum_down_time = single_row.minimum_down_time
        if (
            start_of_curr_constraint - minimum_down_time + 1 <=
            start_of_this <=
            start_of_curr_constraint
        )
            sum = sum + shut_downs[single_row.id]
        end
    end
    return sum
end

function _append_available_units_data_simple(connection, table_name)
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

function _append_available_units_data_compact(connection, table_name)
    return DuckDB.query(
        connection,
        "SELECT
            cons.id,
            ANY_VALUE(cons.asset) AS asset,
            ANY_VALUE(cons.year) AS year,
            ANY_VALUE(cons.rep_period) AS rep_period,
            ANY_VALUE(cons.time_block_start) AS time_block_start,
            ANY_VALUE(cons.time_block_end) AS time_block_end,
            ARRAY_AGG(expr_avail.id) AS avail_indices,
        FROM cons_$table_name AS cons
        LEFT JOIN expr_available_asset_units_compact_method AS expr_avail
            ON cons.asset = expr_avail.asset
            AND cons.year = expr_avail.milestone_year
        LEFT JOIN asset
            ON cons.asset = asset.asset
        WHERE asset.investment_method = 'compact'
        GROUP BY cons.id
        ORDER BY cons.id
        ",
    )
end
