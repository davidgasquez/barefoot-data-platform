-- dataset.name = another_transformed_numbers
-- dataset.schema = raw
-- dataset.depends = raw.transformed_numbers
select
    *
from raw.transformed_numbers
where total > 2
