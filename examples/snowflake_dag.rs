use sqlparser::ast::*;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};

fn main() {
    let sql = r#"
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', o.order_date) AS month,
        c.customer_id,
        c.region,
        SUM(oi.quantity * p.price) AS revenue
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY 1, 2, 3
),
top_customers AS (
    SELECT
        customer_id,
        region,
        SUM(revenue) AS total_revenue,
        COUNT(month) AS active_months
    FROM monthly_sales
    GROUP BY customer_id, region
    HAVING COUNT(month) >= 6
)
SELECT
    tc.region,
    tc.customer_id,
    c.customer_name,
    tc.total_revenue,
    tc.active_months,
    RANK() OVER (PARTITION BY tc.region ORDER BY tc.total_revenue DESC) AS region_rank
FROM top_customers tc
JOIN customers c ON tc.customer_id = c.customer_id
WHERE tc.total_revenue > 50000
ORDER BY tc.region, region_rank
"#;

    let dialect = SnowflakeDialect {};
    let ast = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");

    // Create a DAG from the parsed SQL
    let mut dag = QueryDAG::new();

    for statement in &ast {
        if let Statement::Query(query) = statement {
            dag.build_from_query(query);
        }
    }

    dag.print();
}

#[derive(Debug, Clone)]
struct TableNode {
    name: String,
    alias: Option<String>,
    node_type: NodeType,
}

#[derive(Debug, Clone)]
enum NodeType {
    BaseTable,
    CTE,
    Subquery,
}

#[derive(Debug, Clone)]
struct ColumnNode {
    name: String,
    source_table: Option<String>,
    expression: String,
    dependencies: Vec<String>,
}

#[derive(Debug)]
struct QueryDAG {
    tables: HashMap<String, TableNode>,
    columns: Vec<ColumnNode>,
    dependencies: HashMap<String, HashSet<String>>,
}

impl QueryDAG {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            columns: Vec::new(),
            dependencies: HashMap::new(),
        }
    }

    fn build_from_query(&mut self, query: &Query) {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let cte_name = cte.alias.name.value.clone();
                self.tables.insert(
                    cte_name.clone(),
                    TableNode {
                        name: cte_name.clone(),
                        alias: None,
                        node_type: NodeType::CTE,
                    },
                );

                if let SetExpr::Select(select) = cte.query.body.as_ref() {
                    self.extract_from_select(&cte_name, select);
                }
            }
        }

        if let SetExpr::Select(select) = query.body.as_ref() {
            self.extract_from_select("__result__", select);
        }
    }

    fn extract_from_select(&mut self, context: &str, select: &Select) {
        for table_with_joins in &select.from {
            self.extract_table(&table_with_joins.relation);

            for join in &table_with_joins.joins {
                self.extract_table(&join.relation);
            }
        }

        for proj in &select.projection {
            match proj {
                SelectItem::UnnamedExpr(expr) => {
                    let deps = Self::extract_column_deps(expr);
                    self.columns.push(ColumnNode {
                        name: format!("{}", expr),
                        source_table: Some(context.to_string()),
                        expression: format!("{}", expr),
                        dependencies: deps,
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let deps = Self::extract_column_deps(expr);
                    self.columns.push(ColumnNode {
                        name: alias.value.clone(),
                        source_table: Some(context.to_string()),
                        expression: format!("{}", expr),
                        dependencies: deps,
                    });
                }
                SelectItem::Wildcard(_) => {
                    self.columns.push(ColumnNode {
                        name: "*".to_string(),
                        source_table: Some(context.to_string()),
                        expression: "*".to_string(),
                        dependencies: vec![],
                    });
                }
                _ => {}
            }
        }
    }

    fn extract_table(&mut self, factor: &TableFactor) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = format!("{}", name);
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                let key = alias_name.clone().unwrap_or_else(|| table_name.clone());

                self.tables.insert(
                    key.clone(),
                    TableNode {
                        name: table_name.clone(),
                        alias: alias_name,
                        node_type: NodeType::BaseTable,
                    },
                );

                self.dependencies.entry(key).or_insert_with(HashSet::new);
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                if let Some(table_alias) = alias {
                    let alias_name = table_alias.name.value.clone();
                    self.tables.insert(
                        alias_name.clone(),
                        TableNode {
                            name: format!("(subquery)"),
                            alias: Some(alias_name.clone()),
                            node_type: NodeType::Subquery,
                        },
                    );

                    if let SetExpr::Select(select) = subquery.body.as_ref() {
                        self.extract_from_select(&alias_name, select);
                    }
                }
            }
            _ => {}
        }
    }

    fn extract_column_deps(expr: &Expr) -> Vec<String> {
        let mut deps = Vec::new();
        Self::walk_expr(expr, &mut deps);
        deps
    }

    fn walk_expr(expr: &Expr, deps: &mut Vec<String>) {
        match expr {
            Expr::Identifier(ident) => {
                deps.push(ident.value.clone());
            }
            Expr::CompoundIdentifier(idents) => {
                deps.push(format!(
                    "{}",
                    idents
                        .iter()
                        .map(|i| i.value.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                ));
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::walk_expr(left, deps);
                Self::walk_expr(right, deps);
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            Self::walk_expr(e, deps);
                        }
                    }
                }
            }
            Expr::Nested(e) => Self::walk_expr(e, deps),
            _ => {}
        }
    }

    fn print(&self) {
        println!("TABLES:");
        for (key, table) in &self.tables {
            println!("  {} [{:?}]", key, table.node_type);
            if let Some(alias) = &table.alias {
                println!("    alias: {}", alias);
            }
            println!("    full_name: {}", table.name);
        }

        println!("\nCOLUMNS:");
        for col in &self.columns {
            println!("  {}", col.name);
            if let Some(table) = &col.source_table {
                println!("    context: {}", table);
            }
            println!("    expr: {}", col.expression);
            if !col.dependencies.is_empty() {
                println!("    deps: {:?}", col.dependencies);
            }
        }

        println!("\nDEPENDENCY GRAPH:");
        let mut sorted_tables: Vec<_> = self.tables.keys().collect();
        sorted_tables.sort();
        for table in sorted_tables {
            println!("  {} -> depends on tables in FROM/JOIN", table);
        }

        println!("\n=== TODO ===");
        println!("1. Build topological sort for execution order");
        println!("2. Resolve column names to specific tables");
        println!("3. Track column lineage through CTEs");
        println!("4. Add schema validation");
        println!("5. Detect circular dependencies");
        println!("6. Build column-level lineage graph");
    }
}
