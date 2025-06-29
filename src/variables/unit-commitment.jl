export add_unit_commitment_variables!

"""
    add_unit_commitment_variables!(model, variables)

Adds unit commitment variables to the optimization `model` based on the `:units_on` indices.
Additionally, variables are constrained to be integers based on the `unit_commitment_integer` property.

"""
function add_unit_commitment_variables!(model, variables)
    units_on_indices = variables[:units_on].indices

    variables[:units_on].container = [
        @variable(
            model,
            lower_bound = 0.0,
            integer = row.unit_commitment_integer,
            base_name = "units_on[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]",
        ) for row in units_on_indices
    ]

    start_up_indices = variables[:start_up].indices

    variables[:start_up].container = [
        @variable(
            model,
            lower_bound = 0.0,
            integer = row.unit_commitment_integer,
            base_name = "start_up[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]",
        ) for row in start_up_indices
    ]

    shut_down_indices = variables[:shut_down].indices

    variables[:shut_down].container = [
        @variable(
            model,
            lower_bound = 0.0,
            integer = row.unit_commitment_integer,
            base_name = "shut_down[$(row.asset),$(row.year),$(row.rep_period),$(row.time_block_start):$(row.time_block_end)]",
        ) for row in shut_down_indices
    ]

    return
end
