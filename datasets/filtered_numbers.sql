-- dataset.name = filtered_numbers
-- dataset.schema = raw
-- dataset.depends = raw.transformed_numbers
select
    value,
    square,
    label,
    double,
    parity
from raw.transformed_numbers
where value >= 3
