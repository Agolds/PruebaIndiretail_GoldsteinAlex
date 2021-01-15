from utils.actions import Actions
from utils.spark_util import SparkUtil

from main_c.program_functions import ProgramFunctions

import json


# import argparse


def main(spark, data, actions, functions):
    dataframes = {}

    for file in data["source_files"]:
        file_df = spark_object.read_data(spark, file["source_path"], file["file_format"])
        dataframes[file["source_name"]] = file_df

    for action in data["actions"].items():
        if action[1]["type"] == "join":
            joined = actions.join_df(dataframes[action[1]["left_df"]]
                                     , dataframes[action[1]["right_df"]]
                                     , action[1]["condition"]
                                     , action[1]["how"])

            if action[1]["selected_columns"] is not None and action[1]["selected_columns"] != "*":
                joined = joined.select(action[1]["selected_columns"].split(','))
            else:
                joined = joined.select("*")

            dataframes[action[0]] = joined

        elif action[1]["type"] == "aggregation":
            agg = actions.aggregations(dataframes[action[1]["df_to_aggregate"]]
                                       , action[1]["type_agg"]
                                       , action[1]["groupBy_columns"]
                                       , action[1]["agg_columns"])
            dataframes[action[0]] = agg

        elif action[1]["type"] == "filter_df":
            df_filtered = actions.filter_df(dataframes[action[1]["df_to_filter"]]
                                            , action[1]["condition"])
            dataframes[action[0]] = df_filtered

        elif action[1]["type"] == "window_df":
            window_df = actions.window_df(dataframes[action[1]["df_to_aggregate"]]
                                          , action[1]["partitionBy_columns"]
                                          , action[1]["orderBy_columns"]
                                          , action[1]["agg_columns"]
                                          , action[1]["type_agg"]
                                          , action[1]["result_colName"])
            dataframes[action[0]] = window_df

        elif action[1]["type"] == "add_column":
            new_column = actions.add_replace_column(dataframes[action[1]["df_to_update"]]
                                                    , action[1]["col_name"]
                                                    , action[1]["value_column"]
                                                    , action[1]["column_type"])
            dataframes[action[0]] = new_column

        elif action[1]["type"] == "union_df":
            union = actions.union_df(dataframes[action[1]["df_top"]]
                                     , dataframes[action[1]["df_bottom"]]
                                     , action[1]["columns"])
            dataframes[action[0]] = union

        elif action[1]["type"] == "custom":
            custom_function = getattr(functions, action[1]["function_name"])
            custom_df = custom_function(dataframes[action[1]["df"]], action[1]["params"])

            dataframes[action[0]] = custom_df

        elif action[1]["type"] == "write_df":
            spark_object.save_data(dataframes[action[1]["df_to_write"]]
                                   , action[1]["output_path"]
                                   )


if __name__ == '__main__':
    spark_object = SparkUtil()
    spark = spark_object.get_spark_context(app_name="Indiretail")

    actions = Actions()
    functions = ProgramFunctions()

    # parser = argparse.ArgumentParser()
    #
    # parser.add_argument(
    #     "--file_properties", action="store", help="properties file path", required=True,
    # )

    # args = parser.parse_args()

    # args.file_properties
    with open("../resources/files.json") as file_properties:  #
        data = json.load(file_properties)

    main(spark, data, actions, functions)
