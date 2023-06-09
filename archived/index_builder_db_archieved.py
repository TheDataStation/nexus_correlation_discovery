def create_idx_tbls(self, df, tbl_id, t_attrs_success, s_attrs_success):
    # maintain the mapping between index tbl name to df
    idx_name_to_df = {}
    for t_attr in t_attrs_success:
        for t_granu in T_GRANU:
            t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
            df_idx = df[t_attr_granu].dropna().drop_duplicates()
            self.del_tbl("{}_{}_{}".format(tbl_id, t_attr, t_granu.name))
            idx_name_to_df["{}_{}_{}".format(tbl_id, t_attr, t_granu.value)] = df_idx

    for s_attr in s_attrs_success:
        for s_granu in S_GRANU:
            s_attr_granu = "{}_{}".format(s_attr, s_granu.value)
            df_idx = df[s_attr_granu].dropna().drop_duplicates()
            self.del_tbl("{}_{}_{}".format(tbl_id, s_attr, s_granu.name))
            idx_name_to_df["{}_{}_{}".format(tbl_id, s_attr, s_granu.value)] = df_idx

    for t_attr in t_attrs_success:
        for s_attr in s_attrs_success:
            for t_granu in T_GRANU:
                for s_granu in S_GRANU:
                    t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                    s_attr_granu = "{}_{}".format(s_attr, s_granu.value)

                    df_idx = df[[t_attr_granu, s_attr_granu]].dropna().drop_duplicates()
                    idx_name_to_df[
                        "{}_{}_{}_{}_{}".format(
                            tbl_id, t_attr, t_granu.value, s_attr, s_granu.value
                        )
                    ] = df_idx

    for idx_name, df in idx_name_to_df.items():
        self.ingest_df_to_db(df, idx_name)


def get_agg_df(self, tbl_id, num_columns, units):
    # create variables, only consider avg and count now
    vars = [
        Variable(attr, AggFunc.AVG, "{}_{}".format(attr, "avg")) for attr in num_columns
    ]
    vars.append(Variable("*", AggFunc.COUNT, "count"))
    df_agg = self.db_search.transform(tbl_id, units, vars)
    return df_agg


def create_agg_tbl(self, df, tbl_id, t_attrs_success, s_attrs_success, t_attrs):
    idx_name_to_df = {}
    num_columns = self.get_numerical_columns(tbl_id, t_attrs)
    for t_attr in t_attrs_success:
        for t_granu in T_GRANU:
            units = [Unit(t_attr, t_granu)]
            df_agg = self.get_agg_df(tbl_id, num_columns, units)
            idx_name_to_df["{}_{}_{}".format(tbl_id, t_attr, t_granu.value)] = df_agg

    for s_attr in s_attrs_success:
        for s_granu in S_GRANU:
            units = [Unit(s_attr, s_granu)]
            df_agg = self.get_agg_df(tbl_id, num_columns, units)
            idx_name_to_df["{}_{}_{}".format(tbl_id, s_attr, s_granu.value)] = df_agg

    for t_attr in t_attrs_success:
        for s_attr in s_attrs_success:
            for t_granu in T_GRANU:
                for s_granu in S_GRANU:
                    units = [Unit(t_attr, t_granu), Unit(s_attr, s_granu)]
                    df_agg = self.get_agg_df(tbl_id, num_columns, units)
                    idx_name_to_df[
                        "{}_{}_{}_{}_{}".format(
                            tbl_id, t_attr, t_granu.value, s_attr, s_granu.value
                        )
                    ] = df_agg

    for idx_name, df in idx_name_to_df.items():
        self.ingest_df_to_db(df, idx_name)


def create_indices_on_tbl(self, tbl_id, t_attrs, s_attrs):
    self.create_index_on_unary_attr(tbl_id, t_attrs, T_GRANU, "t")
    self.create_index_on_unary_attr(tbl_id, s_attrs, S_GRANU, "s")
    self.create_index_on_binary_attrs(tbl_id, t_attrs, s_attrs)


def create_index_on_unary_attr(self, tbl_id, attrs, resolutions, type):
    sql_str = """
        CREATE INDEX {idx_name} on {tbl} using hash ({field});
    """

    for i, attr in enumerate(attrs):
        for granu in resolutions:
            attr_name = "{}_{}".format(attr, granu.value)
            # execute creating index
            self.cur.execute(
                sql.SQL(sql_str).format(
                    idx_name=sql.Identifier(
                        "{}_{}{}_{}_idx".format(tbl_id, type, i, granu.value)
                    ),
                    tbl=sql.Identifier(tbl_id),
                    field=sql.Identifier(attr_name),
                )
            )

            idx_tbl_name = "{}_{}_{}".format(tbl_id, attr, granu.value)
            self.cur.execute(
                sql.SQL("CREATE unique INDEX {idx_name} on {tbl} ({field});").format(
                    idx_name=sql.Identifier(
                        "{}_{}{}_{}_idx2".format(tbl_id, type, i, granu.value)
                    ),
                    tbl=sql.Identifier(idx_tbl_name),
                    field=sql.Identifier(attr_name),
                )
            )

    def create_index_on_binary_attrs(self, tbl_id, t_attrs, s_attrs):
        sql_str = """
            CREATE INDEX {idx_name} ON {tbl} ({field1}, {field2});
        """
        for i, t_attr in enumerate(t_attrs):
            for j, s_attr in enumerate(s_attrs):
                for t_granu in T_GRANU:
                    for s_granu in S_GRANU:
                        t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                        s_attr_granu = "{}_{}".format(s_attr, s_granu.value)

                        query = sql.SQL(sql_str).format(
                            idx_name=sql.Identifier(
                                "{}_{}_{}_idx".format(
                                    tbl_id,
                                    "{}_{}".format(i, t_granu.value),
                                    "{}_{}".format(j, s_granu.value),
                                )
                            ),
                            tbl=sql.Identifier(tbl_id),
                            field1=sql.Identifier(t_attr_granu),
                            field2=sql.Identifier(s_attr_granu),
                        )
                        # print(self.cur.mogrify(query))
                        self.cur.execute(query)

                        idx_tbl_name = "{}_{}_{}_{}_{}".format(
                            tbl_id, t_attr, t_granu.value, s_attr, s_granu.value
                        )

                        query = sql.SQL(
                            "CREATE unique {idx_name} ON {tbl} ({field1}, {field2});"
                        ).format(
                            idx_name=sql.Identifier(
                                "{}_{}_{}_{}_{}_idx2".format(
                                    tbl_id, i, t_granu.value, j, s_granu.value
                                )
                            ),
                            tbl=sql.Identifier(idx_tbl_name),
                            field1=sql.Identifier(t_attr_granu),
                            field2=sql.Identifier(s_attr_granu),
                        )
                        # print(self.cur.mogrify(query))
                        self.cur.execute(query)

        # find the number of test times to correct for multiple comparison problem
        # the number of tests equals the sum of numerical column numbers of aligned tables + 1
        # plus 1 is because we calculate count for each table
        # test_num = 0
        # for tbl_info in aligned_tbls:
        #     tbl2, units2 = (
        #         tbl_info[0],
        #         tbl_info[2],
        #     )
        #     attrs2 = [unit.attr_name for unit in units2]
        #     if (tbl1, tuple(attrs1), tbl2, tuple(attrs2)) not in self.visited:
        #         test_num += len(tbl_attrs[tbl_info[0]]["num_columns"]) + 1

        # if self.correct_method == "Bonferroni":
        #     if test_num != 0:
        #         p_t = p_t / test_num
        #         print("p value", p_t)
