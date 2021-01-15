from utils.actions import Actions


class ProgramFunctions:
    @staticmethod
    def interval_stock(df, params):
        """
        Calculates interval stock of table
        :param df:
        :param params:
        :return:
        """
        par_cols = params["par_cols"]
        order_cols = params["order_cols"]
        agg_cols = params["agg_cols"]
        col_name = "count_per_key"
        type_agg = "count"

        condition = params["filter_condition"]

        df_agg = Actions.window_df(df, par_cols, order_cols, agg_cols, type_agg, col_name)
        df_agg = Actions.filter_df(df_agg, condition)

        return df_agg
