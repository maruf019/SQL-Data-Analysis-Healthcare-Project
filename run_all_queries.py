# import logging
# import os
# from query_group_by import query_group_by
# from query_inner_join import query_inner_join
# from query_left_join import query_left_join
# from query_right_join import query_right_join
# from query_full_join import query_full_join
# from query_self_join import query_self_join
# from query_union import query_union
# # from query_group_by_new import query_group_by_new
# # from query_having import query_having
# # from query_exists import query_exists
# # from query_any_all import query_any_all
# # from query_select_into import query_select_into
# # from query_insert_into_select import query_insert_into_select
# # from query_case import query_case
# # from query_null_functions import query_null_functions
# # from query_stored_procedure import query_stored_procedure
# # from query_comments_operators import query_comments_operators

# logging.basicConfig(
#     filename='run_all_queries.log',
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# def run_all_queries(db_name='healthcare.db'):
#     """Execute all SQL query scripts."""
#     queries = [
#         ('Group By', query_group_by),
#         ('Inner Join', query_inner_join),
#         ('Left Join', query_left_join),
#         ('Right Join', query_right_join),
#         ('Full Join', query_full_join),
#         ('Self Join', query_self_join),
#         ('Union', query_union),
#         ('Group By New', query_group_by_new),
#         ('Having', query_having),
#         ('Exists', query_exists),
#         ('Any/All', query_any_all),
#         ('Select Into', query_select_into),
#         ('Insert Into Select', query_insert_into_select),
#         ('Case', query_case),
#         ('Null Functions', query_null_functions),
#         ('Stored Procedure', query_stored_procedure),
#         ('Comments and Operators', query_comments_operators)
#     ]

#     for query_name, query_func in queries:
#         try:
#             logging.info(f"Running {query_name} query...")
#             print(f"\nRunning {query_name} query...")
#             query_func(db_name)
#             logging.info(f"{query_name} query completed.")
#             print(f"{query_name} query completed.")
#         except Exception as e:
#             logging.error(f"{query_name} query failed: {e}")
#             print(f"{query_name} query failed: {e}")
#             raise

# if __name__ == "__main__":
#     try:
#         run_all_queries()
#         logging.info("All queries executed successfully.")
#         print("\nAll queries executed successfully.")
#     except Exception as e:
#         logging.error(f"Failed to execute all queries: {e}")
#         print(f"Failed to execute all queries: {e}")
