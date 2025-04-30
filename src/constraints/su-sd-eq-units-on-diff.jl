export add_su_sd_eq_units_on_diff_constraints!

"""
    add_su_sd_eq_units_on_diff_constraints!(model, constraints)

Adds the start up - shut down = units_on difference to the model.
"""
function add_su_sd_eq_units_on_diff_constraints!(
    connection,
    model,
    variables,
    expressions,
    constraints,
)
    let table_name = :su_sd_eq_units_on_diff, cons = constraints[:su_sd_eq_units_on_diff]
        startup_container = []
        shutdown_container = []
        units_on_now_container = []
        last_asset = nothing
        for (i, (ind, su, sd, uo)) in enumerate(
            zip(
                variables[:units_on].indices,
                variables[:start_up].container,
                variables[:shut_down].container,
                variables[:units_on].container,
            ),
        )
            if (ind.asset == last_asset)
                push!(startup_container, su)
                push!(shutdown_container, sd)
                push!(units_on_now_container, uo)
            end
            last_asset = ind.asset
        end

        units_on_prev_container = []

        indices = collect(variables[:units_on].indices)
        container = collect(variables[:units_on].container)

        for i in (1:(length(indices)-1))
            if indices[i].asset == indices[i+1].asset
                push!(units_on_prev_container, container[i])
            end
        end

        attach_constraint!(
            model,
            cons,
            table_name,
            [
                @constraint(
                    model,
                    units_on_now - units_on_prev == start_up - shut_down,
                    base_name = "$table_name[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]"
                ) for (row, start_up, shut_down, units_on_now, units_on_prev) in zip(
                    cons.indices,
                    startup_container,
                    shutdown_container,
                    units_on_now_container,
                    units_on_prev_container,
                )
            ],
        )
    end
end
