{{ config(
    materialized='table'
) }}

/*
    Demonstration of a recursive CTE to build the full hierarchy of categories.
    Each record returns the category ID, its parent, the depth in the hierarchy,
    the root category and a concatenated path showing the lineage.
    This structure can be used to roll up metrics from leaf categories up to
    their parents or for navigation of category trees.
*/

with recursive category_tree as (
    -- Anchor members: top-level categories (no parent)
    select
        category_id
      , parent_id
      , category_name
      , category_id as root_category_id
      , category_name as root_category_name
      , 0 as depth
      , cast(category_name as string) as category_path
    from {{ ref('stg_categories') }}
    where parent_id is null

    union all

    -- Recursive member: join children to their parent path
    select
        c.category_id
      , c.parent_id
      , c.category_name
      , category_tree.root_category_id
      , category_tree.root_category_name
      , category_tree.depth + 1 as depth
      , concat(category_tree.category_path, ' > ', c.category_name) as category_path
    from {{ ref('stg_categories') }} c
    join category_tree
        on c.parent_id = category_tree.category_id
)

select
    category_id
  , parent_id
  , category_name
  , root_category_id
  , root_category_name
  , depth
  , category_path
from category_tree
order by root_category_id, depth, category_id