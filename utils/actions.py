from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import max as max_


class Actions:
    @staticmethod
    def join_df(left_df, right_df, on=None, how=None):
        """
        Joins dataframes
        :param left_df: left dataframe
        :param right_df: right dataframe
        :param on: join condition
        :param how: join type
        :return: Joined dataframe
        """

        join = left_df.join(right_df, on=[on], how=how)

        return join

    @staticmethod
    def aggregations(df, type_agg, groupby_cols, columns_agg):
        """
        Does aggregations to dataframes
        :param groupby_cols: columns for which the dataframe is grouped
        :param columns_agg: columns to be added in the aggregation
        :param type_agg: type of aggregation
        :param df: dataframe to aggregate
        :return: Aggregated dataframe
        """

        df_agg = df.groupBy(groupby_cols.split(","))

        if type_agg == "sum":
            df_agg = df_agg.sum(columns_agg)
            for col in columns_agg.split(","):
                df_agg = df_agg.withColumnRenamed(f"SUM({col})", col)

        elif type_agg == "count":
            df_agg = df_agg.count(columns_agg)
            for col in columns_agg.split(","):
                df_agg = df_agg.withColumnRenamed(f"COUNT({col})", col)

        elif type_agg == "max":
            df_agg = df_agg.agg(max_(columns_agg))
            for col in columns_agg.split(","):
                df_agg = df_agg.withColumnRenamed(f"MAX({col})", col)

        else:
            return df_agg

        return df_agg

    @staticmethod
    def filter_df(df, condition):
        """
        Filters dataframe records by given condition
        :param df: raw dataframe
        :param condition: condition to be applied in dataframe
        :return: Filtered dataframe
        """

        return df.where(condition)

    @staticmethod
    def window_df(df, par_cols, order_cols, agg_cols, type_agg, col_name):
        """
        Does a window function over dataframe
        :param col_name:
        :param agg_cols:
        :param df:
        :param par_cols:
        :param order_cols:
        :param type_agg:
        :return:
        """

        par_cols = par_cols.split(",")
        order_cols = order_cols.split(",")
        w = (Window.partitionBy(par_cols).orderBy(order_cols)
             .rangeBetween(Window.unboundedPreceding, 0))

        if type_agg == "sum":
            df_agg = df.withColumn(col_name, F.sum(agg_cols).over(w))
        elif type_agg == "count":
            df_agg = df.withColumn(col_name, F.count(agg_cols).over(w))
        else:
            return df

        return df_agg

    @staticmethod
    def add_replace_column(df, col_name, default_value, col_type):
        """
        Adds or replace values from specific column
        :param col_type:
        :param df:
        :param col_name:
        :param default_value:
        :return:
        """

        return df.withColumn(col_name, F.lit(default_value))  # .cast(col_type)

    @staticmethod
    def union_df(top_df, bottom_df, cols):
        """
        Unions dataframes resolving columns by name, not by position
        :param cols:
        :param top_df:
        :param bottom_df:
        :return:
        """
        cols = cols.split(",")

        top_df = top_df.select(cols)
        bottom_df = bottom_df.select(cols)

        return top_df.unionByName(bottom_df)
