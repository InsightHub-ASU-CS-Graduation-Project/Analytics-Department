

def get_home_page_config(analyzer):
    return {
        "Total Jobs": lambda req: analyzer.get_kpi_value(
            column = 'id', 
            agg_func = 'count',
            round_value = 0,
            filters = req.filters, 
            title = "Posted recently",
            suffix = "Jobs",
            type = "card"
        ),


       "Avg Jobs per Hour": lambda req: (
            res := analyzer.get_time_series(
                date_col = "created",
                time_period = "H",
                filters = req.filters,
                rules = req.rules,
                orient = req.orient if req.orient else 'records'
            ),
            data_list := res.get('data', []),
            
            {
                "data": round(sum(d['y'] for d in data_list) / len(data_list), 0) if data_list else 0,
                "title": "Avg Jobs per Hour",
                "suffix": " /h",

                "trend": (
                    "up" if data_list[-1]['y'] > data_list[-2]['y'] else "down"
                ) if len(data_list) > 1 else "neutral",

                "type": "card",
            }
        )[-1],

        "Hourly Postings Trend": lambda req: analyzer.get_time_series(
            date_col = "created",
            time_period = "H",
            filters = req.filters,
            rules = req.rules,
            orient = req.orient if req.orient else 'records',
            title = "Job Postings Trend (Hourly)",
            type = "line",
            tooltip_format = "{point.y} Jobs posted at {point.x}",
            description = "Trace the rhythm of recruitment. " \
            "This chart reveals the exact peaks and valleys of market activity over time."
        ),


        "Seniority Level Distribution": lambda req: analyzer.get_categorical_distribution(
                x_col = "seniority_level",
                top_n = 3,
                show_others = False,
                filters = req.filters,
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "Requested Seniority Distribution",
                tooltip_format = "Level: {point.x}\nCount: {point.y} Jobs",
                show_data_labels = True,
                show_legend = True,
                type = "doughnut",
                description = "Are companies seeking fresh talent or seasoned leaders? " \
                "Discover the exact experience levels shaping today's dynamic workforce."
        ),


        "Salaries by Seniority": lambda req: (
            res := analyzer.get_numerical_aggregation(
                x_col = "seniority_level",
                y_col = "salary_max",
                agg_func = "mean",
                round_value = 0,
                top_n = 5,
                show_others = True,
                filters = {**(req.filters or {}), "seniority_level": {"!=": "Not Specified"}},
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "Average Salary by Seniority",
                type = "column",
                description = "Follow the money. " \
                "Trace the financial journey from entry-level beginnings all the way up to the executive suites."
            ),

            [
                row.update({
                    "others_color": "#9E9E9E",

                    "others_tooltip": "Includes: " + "\n".join(
                        [f"{item['x']}: {item['y']}" for item in row.get("others_breakdown", [])]
                    )
                }) 
                for row in res.get("data", []) if row.get("is_others") == True
            ],

            res
        )[-1],


        "Top 10 Jobs": lambda req: analyzer.get_categorical_distribution(
            x_col = "title",
            top_n = 10,
            show_others = False,
            filters = req.filters,
            rules = req.rules,
            orient = req.orient if req.orient else 'records',
            title = "Most Demanded Roles",
            type = "treemap",
            description = "Step into the spotlight. " \
            "These are the highly sought-after roles currently dominating the global talent hunt."
        ),


        "Top Companies Overview": lambda req: (
            res := analyzer.get_categorical_distribution(
                x_col = "company_name",
                top_n = 7,
                show_others = False,
                filters = req.filters,
                orient = req.orient if req.orient else 'records',
                title = "Top 7 Companies: Postings vs Total Salary",
                type = "grouped bar",
                description = "Meet the market heavyweights. " \
                "See who isn't just hiring the most, but also investing the heaviest financial capital."
            ),

            [
                row.update({
                    "y_jobs_count": row.pop('y'),

                    "y_total_salary": int(
                        analyzer.get_kpi_value(
                            column = "salary_max",
                            agg_func = "sum",
                            round_value = 0,
                            filters = {**(req.filters or {}), "company_name": row['x']}
                        ).get("data") or 0
                    )
                }) 
                for row in res.get("data", [])
            ],

            res
        )[-1],
        
    }



def get_explore_page_config(analyzer):
    return {
        "Total Jobs": lambda req: analyzer.get_kpi_value(
            column = 'id', 
            agg_func = 'count',
            round_value = 0,
            filters = req.filters, 
            title = "Posted Laterly",
            suffix = "Jobs",
            type = "card"
        ),


      "Avg Jobs per Hour": lambda req: (
            res := analyzer.get_time_series(
                date_col = "created",
                time_period = "H",
                filters = req.filters,
                rules = req.rules,
                orient = req.orient if req.orient else 'records'
            ),
            data_list := res.get('data', []),
            
            {
                "data": round(sum(d['y'] for d in data_list) / len(data_list), 0) if data_list else 0,
                "title": "Avg Jobs per Hour",
                "suffix": " /h",

                "trend": (
                    "up" if data_list[-1]['y'] > data_list[-2]['y'] else "down"
                ) if len(data_list) > 1 else "neutral",

                "type": "card",
            }
        )[-1],

        "Hourly Postings Trend": lambda req: analyzer.get_time_series(
            date_col = "created",
            time_period = "H",
            filters = req.filters,
            rules = req.rules,
            orient = req.orient if req.orient else 'records',
            title = "Job Postings Trend (Hourly)",
            type = "line",
            tooltip_format = "{point.y} Jobs posted at {point.x}",
            description = "Trace the rhythm of recruitment. " \
            "This chart reveals the exact peaks and valleys of market activity over time."
        ),


        "Top 10 Jobs": lambda req: (
            res := analyzer.get_categorical_distribution(
                x_col = "title",
                top_n = 10,
                show_others = False,
                filters = req.filters,
                orient = req.orient if req.orient else 'records',
                title = "Job Roles by Category",
                type = "hier treemap",
                description = "Step into the spotlight. " \
            "These are the highly sought-after roles currently dominating the global talent hunt."
            ),

            [
                row.update({
                    "x_parent": next(
                        (item['x'] for item in analyzer.get_categorical_distribution(
                            x_col = "category_label",
                            top_n = 1,
                            filters = {"title": row['x']} 
                        ).get("data", [])),
                        "Unknown"
                    )
                }) 
                for row in res.get("data", [])
            ],

            res
        )[-1],


        "Top Companies Overview": lambda req: (
            res := analyzer.get_categorical_distribution(
                x_col = "company_name",
                top_n = 7,
                show_others = False,
                filters = req.filters,
                orient = req.orient if req.orient else 'records',
                title = "Top 7 Companies: Postings vs Total Salary",
                type = "multiple bar",
                description = "Meet the market heavyweights. " \
                "See who isn't just hiring the most, but also investing the heaviest financial capital."
            ),

            [
                row.update({
                    "y_jobs_count": row.pop('y'),

                    "y_total_salary": int(
                        analyzer.get_kpi_value(
                            column = "salary_max",
                            agg_func = "sum",
                            round_value = 0,
                            filters = {**(req.filters or {}), "company_name": row['x']}
                        ).get("data") or 0
                    )
                }) 
                for row in res.get("data", [])
            ],
            
            res
        )[-1],

    }
