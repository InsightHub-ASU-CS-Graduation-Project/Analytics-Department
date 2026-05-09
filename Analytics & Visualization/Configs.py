import pandas as pd

def get_home_page_config(analyzer):
    return {
        "Total Jobs": lambda req: analyzer.get_kpi_value(
            column = 'id', 
            agg_func = 'count',
            round_value = 0,
            filters = req.filters, 
            title = "Job Drop",
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
                "title": "Jobs per Hour",
                "suffix": "jobs/h",
                "type": "card",
            }
        )[-1],


        "Market Share Percentage": lambda req: (
            filtered_jobs := int(
                analyzer.get_kpi_value(
                    column = 'id', 
                    agg_func = 'count',
                    round_value = 0,
                    filters = req.filters
                ).get("data") or 0
            ),

            total_jobs := int(
                analyzer.get_kpi_value(
                    column = 'id', 
                    agg_func = 'count',
                    round_value = 0,
                    filters = None
                ).get("data") or 1
            ),

            {
                "data": round((filtered_jobs / total_jobs) * 100, 1) if total_jobs > 0 else 0,
                "title": "The Industry Weight",
                "suffix": "%",
                "type": "card"
            }
        )[-1],


        "Experience Multiplier": lambda req: (
            salaries_agg := analyzer.get_numerical_aggregation(
                x_col = "seniority_level",
                y_col = "salary_avg",
                agg_func = "mean",
                round_value = 0,
                filters = req.filters
            ).get("data", []),

            senior_salary := next((row['y'] for row in salaries_agg if row['x'] == 'Senior'), 0),
            junior_salary := next((row['y'] for row in salaries_agg if row['x'] == 'Junior'), 0),

            {
                "data": round(senior_salary / junior_salary, 1) if junior_salary > 0 else 0,
                "title": "Seniority Salary Perks",
                "suffix": "x",
                "description": "",
                "type": "card"
            }
        )[-1],
        

       "Hourly Postings Trend": lambda req: (
            res := analyzer.get_time_series(
                date_col = "created",
                time_period = "H",
                filters = req.filters,
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "Job Drops on the Radar",
                type = "line",
                tooltip_format = "{point.y} Jobs posted at {point.x}",
                description = "What's been happening while you were busy? Here's a live recap of the new job postings hitting the market over the last 6 hours."
            ),
            
            recent_6_hours := res.get("data", [])[-6:],
            
            [
                row.update({
                    "x": pd.to_datetime(row['x']).strftime('%I %p')
                }) 
                for row in recent_6_hours
            ],
            
            res.update({"data": recent_6_hours}),
            
            res
        )[-1],


        "Seniority Level Distribution": lambda req: analyzer.get_categorical_distribution(
                x_col = "seniority_level",
                top_n = 4,
                show_others = False,
                filters = req.filters,
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "Junior, Senior, or Just Vibes?",
                tooltip_format = "Level: {point.x}\nCount: {point.y} Jobs",
                show_data_labels = True,
                show_legend = True,
                type = "doughnut",
                description = "This shows you the exact percentage of jobs asking for a specific level of experience."
        ),


        "Salaries by Seniority": lambda req: (
            res := analyzer.get_numerical_aggregation(
                x_col = "seniority_level",
                y_col = "salary_max",
                agg_func = "mean",
                round_value = 0,
                top_n = 4,
                show_others = False,
                filters = {**(req.filters or {}), "seniority_level": {"!=": "Not Specified"}},
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "The Salary Tea ☕",
                type = "column",
                description = "Check out the exact average pay for every seniority level, from Intern to Director."
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


        "Top 6 Jobs": lambda req: analyzer.get_categorical_distribution(
            x_col = "title",
            top_n = 6,
            show_others = False,
            filters = req.filters,
            rules = req.rules,
            orient = req.orient if req.orient else 'records',
            title = "Top Roles by Volume",
            type = "treemap",
            description = "No guessing needed. This chart shows you exactly which roles have the most active job postings right now."
        ),


        "Top Hiring Companies": lambda req: analyzer.get_categorical_distribution(
            x_col = "company_name",
            top_n = 5,
            show_others = False,
            filters = req.filters,
            orient = req.orient if req.orient else 'records',
            title = "Top 5 Companies by Job Volume",
            type = "bar",
            tooltip_format = "Company: {point.x}<br>Openings: {point.y} Jobs",
            description = ""
        ),


        "Top Investing Companies": lambda req: analyzer.get_numerical_aggregation(
            x_col = "company_name",
            y_col = "salary_max",
            agg_func = "sum",
            round_value = 0,
            top_n = 5,
            show_others = False,
            filters = {**(req.filters or {}), "company_name": {"!=": "Not Specified"}},
            orient = req.orient if req.orient else 'records',
            title = "Top 5 Companies by Financial Investment",
            type = "bar",
            tooltip_format = "Company: {point.x}<br>Total Investment: ${point.y}",
            description = ""
        ),


        "Salary Growth vs Junior Base": lambda req: (
            res := analyzer.get_numerical_aggregation(
                x_col = "seniority_level",
                y_col = "salary_avg",
                agg_func = "mean",
                round_value = 0,
                filters = {
                    **(req.filters or {}),
                    "seniority_level": ["Junior", "Mid-Level", "Senior", "Lead"]
                },
                orient = req.orient if req.orient else 'records',
                title = "Project Your Growth 🎯",
                type = "line",
                tooltip_format = "Level: {point.x}<br>Multiplier: {point.y}x<br>Avg Salary: ${point.raw_salary}",
                description = "Junior money is just the starting line. See exactly how many times your salary multiplies as you climb to Senior and Lead."
            ),

            data_list := res.get("data", []),

            junior_salary := next((row['y'] for row in data_list if row['x'] == 'Junior'), None),

            level_order := {'Junior': 1, 'Mid-Level': 2, 'Senior': 3, 'Lead': 4},
            sorted_data := sorted(data_list, key = lambda d: level_order.get(d['x'], 99)),

            [
                row.update({
                    "raw_salary": row['y'],
                    "y": round(row['y'] / junior_salary, 1) if junior_salary and junior_salary > 0 else 0,
                    "y_suffix": "x"
                }) 
                for row in sorted_data
            ],

            res.update({"data": sorted_data}),

            res
        )[-1],


    "Gig Economy Trend": lambda req: (
            res := analyzer.get_categorical_distribution(
                x_col = "seniority_level",
                top_n = 5,
                show_others = False,
                filters = {
                    **(req.filters or {}), 
                    "seniority_level": ["Junior", "Mid-Level", "Senior", "Lead"]
                },
                orient = req.orient if req.orient else 'records',
                title = "Gig Economy Trend by Seniority",
                type = "line",
                tooltip_format = "Seniority: {point.x}<br>Freelance/Contract roles: {point.y}%",
                description = ""
            ),

            [
                row.update({
                    "total_jobs": row.pop('y'),
                    
                    "flexible_jobs": int(
                        analyzer.get_kpi_value(
                            column = "id",
                            agg_func = "count",
                            round_value = 0,
                            filters = {
                                **(req.filters or {}), 
                                "seniority_level": row['x'],
                                "contract_type": ["Contract", "Freelance"]
                            }
                        ).get("data") or 0
                    )
                }) 
                for row in res.get("data", [])
            ],

            [
                row.update({
                    "y": round((row['flexible_jobs'] / row['total_jobs']) * 100, 1) if row['total_jobs'] > 0 else 0
                }) 
                for row in res.get("data", [])
            ],

            level_order := {'Junior': 1, 'Mid-Level': 2, 'Senior': 3, 'Lead': 4},
            res.update({
                "data": sorted(res.get("data", []), key = lambda d: level_order.get(d['x'], 99))
            }),

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
            title = "Job Drop",
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
                "title": "Jobs per Hour",
                "suffix": "/h",
                "type": "card",
            }
        )[-1],


        "Market Share Percentage": lambda req: (
            filtered_jobs := int(
                analyzer.get_kpi_value(
                    column = 'id', 
                    agg_func = 'count',
                    round_value = 0,
                    filters = req.filters
                ).get("data") or 0
            ),

            total_jobs := int(
                analyzer.get_kpi_value(
                    column = 'id', 
                    agg_func = 'count',
                    round_value = 0,
                    filters = None
                ).get("data") or 1
            ),

            {
                "data": round((filtered_jobs / total_jobs) * 100, 1) if total_jobs > 0 else 0,
                "title": "The Industry Weight",
                "suffix": "%",
                "type": "card"
            }
        )[-1],


        "Experience Multiplier": lambda req: (
            salaries_agg := analyzer.get_numerical_aggregation(
                x_col = "seniority_level",
                y_col = "salary_avg",
                agg_func = "mean",
                round_value = 0,
                filters = req.filters
            ).get("data", []),

            senior_salary := next((row['y'] for row in salaries_agg if row['x'] == 'Senior'), 0),
            junior_salary := next((row['y'] for row in salaries_agg if row['x'] == 'Junior'), 0),

            {
                "data": round(senior_salary / junior_salary, 1) if junior_salary > 0 else 0,
                "title": "Seniority Salary Perks",
                "suffix": "x",
                "description": "",
                "type": "card"
            }
        )[-1],
        

       "Hourly Postings Trend": lambda req: (
            res := analyzer.get_time_series(
                date_col = "created",
                time_period = "H",
                filters = req.filters,
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "Job Drops on the Radar",
                type = "line",
                tooltip_format = "{point.y} Jobs posted at {point.x}",
                description = "What's been happening while you were busy? Here's a live recap of the new job postings hitting the market over the last 6 hours."
            ),
            
            recent_6_hours := res.get("data", [])[-6:],
            
            [
                row.update({
                    "x": pd.to_datetime(row['x']).strftime('%I %p')
                }) 
                for row in recent_6_hours
            ],
            
            res.update({"data": recent_6_hours}),
            
            res
        )[-1],


        "Seniority Level Distribution": lambda req: analyzer.get_categorical_distribution(
                x_col = "seniority_level",
                top_n = 4,
                show_others = False,
                filters = req.filters,
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "Junior, Senior, or Just Vibes?",
                tooltip_format = "Level: {point.x}\nCount: {point.y} Jobs",
                show_data_labels = True,
                show_legend = True,
                type = "doughnut",
                description = "This shows you the exact percentage of jobs asking for a specific level of experience."
        ),


        "Salaries by Seniority": lambda req: (
            res := analyzer.get_numerical_aggregation(
                x_col = "seniority_level",
                y_col = "salary_max",
                agg_func = "mean",
                round_value = 0,
                top_n = 4,
                show_others = False,
                filters = {**(req.filters or {}), "seniority_level": {"!=": "Not Specified"}},
                rules = req.rules,
                orient = req.orient if req.orient else 'records',
                title = "The Salary Tea ☕",
                type = "column",
                description = "Check out the exact average pay for every seniority level, from Intern to Director."
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


        "Top 6 Jobs": lambda req: analyzer.get_categorical_distribution(
            x_col = "title",
            top_n = 6,
            show_others = False,
            filters = req.filters,
            rules = req.rules,
            orient = req.orient if req.orient else 'records',
            title = "Top Roles by Volume",
            type = "treemap",
            description = "No guessing needed. This chart shows you exactly which roles have the most active job postings right now."
        ),


        "Top Hiring Companies": lambda req: analyzer.get_categorical_distribution(
            x_col = "company_name",
            top_n = 5,
            show_others = False,
            filters = req.filters,
            orient = req.orient if req.orient else 'records',
            title = "Top 5 Companies by Job Volume",
            type = "bar",
            tooltip_format = "Company: {point.x}<br>Openings: {point.y} Jobs",
            description = ""
        ),


        "Top Investing Companies": lambda req: analyzer.get_numerical_aggregation(
            x_col = "company_name",
            y_col = "salary_max",
            agg_func = "sum",
            round_value = 0,
            top_n = 5,
            show_others = False,
            filters = {**(req.filters or {}), "company_name": {"!=": "Not Specified"}},
            orient = req.orient if req.orient else 'records',
            title = "Top 5 Companies by Financial Investment",
            type = "bar",
            tooltip_format = "Company: {point.x}<br>Total Investment: ${point.y}",
            description = ""
        ),


        "Salary Growth vs Junior Base": lambda req: (
            res := analyzer.get_numerical_aggregation(
                x_col = "seniority_level",
                y_col = "salary_avg",
                agg_func = "mean",
                round_value = 0,
                filters = {
                    **(req.filters or {}),
                    "seniority_level": ["Junior", "Mid-Level", "Senior", "Lead"]
                },
                orient = req.orient if req.orient else 'records',
                title = "Project Your Growth 🎯",
                type = "line",
                tooltip_format = "Level: {point.x}<br>Multiplier: {point.y}x<br>Avg Salary: ${point.raw_salary}",
                description = "Junior money is just the starting line. See exactly how many times your salary multiplies as you climb to Senior and Lead."
            ),

            data_list := res.get("data", []),

            junior_salary := next((row['y'] for row in data_list if row['x'] == 'Junior'), None),

            level_order := {'Junior': 1, 'Mid-Level': 2, 'Senior': 3, 'Lead': 4},
            sorted_data := sorted(data_list, key = lambda d: level_order.get(d['x'], 99)),

            [
                row.update({
                    "raw_salary": row['y'],
                    "y": round(row['y'] / junior_salary, 1) if junior_salary and junior_salary > 0 else 0,
                    "y_suffix": "x"
                }) 
                for row in sorted_data
            ],

            res.update({"data": sorted_data}),

            res
        )[-1],


    "Gig Economy Trend": lambda req: (
            res := analyzer.get_categorical_distribution(
                x_col = "seniority_level",
                top_n = 5,
                show_others = False,
                filters = {
                    **(req.filters or {}), 
                    "seniority_level": ["Junior", "Mid-Level", "Senior", "Lead"]
                },
                orient = req.orient if req.orient else 'records',
                title = "Gig Economy Trend by Seniority",
                type = "line",
                tooltip_format = "Seniority: {point.x}<br>Freelance/Contract roles: {point.y}%",
                description = ""
            ),

            [
                row.update({
                    "total_jobs": row.pop('y'),
                    
                    "flexible_jobs": int(
                        analyzer.get_kpi_value(
                            column = "id",
                            agg_func = "count",
                            round_value = 0,
                            filters = {
                                **(req.filters or {}), 
                                "seniority_level": row['x'],
                                "contract_type": ["Contract", "Freelance"]
                            }
                        ).get("data") or 0
                    )
                }) 
                for row in res.get("data", [])
            ],

            [
                row.update({
                    "y": round((row['flexible_jobs'] / row['total_jobs']) * 100, 1) if row['total_jobs'] > 0 else 0
                }) 
                for row in res.get("data", [])
            ],

            level_order := {'Junior': 1, 'Mid-Level': 2, 'Senior': 3, 'Lead': 4},
            res.update({
                "data": sorted(res.get("data", []), key = lambda d: level_order.get(d['x'], 99))
            }),

            res
        )[-1],
        
    }
