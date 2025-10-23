use petgraph::algo::toposort;
use petgraph::graph::DiGraph;
use sqlparser::ast::*;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

fn main() {
    let sql = r#"
WITH monthly_sales AS (
    SELECT DATE_TRUNC('month', o.order_date) AS month, c.customer_id
    FROM orders o JOIN customers c ON o.customer_id = c.customer_id
),
top_customers AS (
    SELECT customer_id, SUM(revenue) AS total FROM monthly_sales GROUP BY customer_id
)
SELECT tc.total FROM top_customers tc JOIN customers c ON tc.customer_id = c.customer_id
"#;

    let ast = Parser::parse_sql(&SnowflakeDialect {}, sql).unwrap();

    println!("=== AST ===\n{:#?}\n", ast);

    let mut dag = DAG::new();
    for stmt in &ast {
        if let Statement::Query(q) = stmt {
            dag.build(q);
        }
    }

    println!("\n=== DAG ===");
    let sorted = toposort(&dag.graph, None).unwrap();
    for idx in sorted {
        let deps: Vec<_> = dag.graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .map(|d| &dag.graph[d])
            .collect();
        if deps.is_empty() {
            println!("{}", dag.graph[idx]);
        } else {
            println!("{} <- {:?}", dag.graph[idx], deps);
        }
    }
}

struct DAG {
    graph: DiGraph<String, ()>,
    nodes: HashMap<String, petgraph::graph::NodeIndex>,
}

impl DAG {
    fn new() -> Self {
        Self { graph: DiGraph::new(), nodes: HashMap::new() }
    }

    fn add(&mut self, name: String) -> petgraph::graph::NodeIndex {
        *self.nodes.entry(name.clone()).or_insert_with(|| self.graph.add_node(name))
    }

    fn build(&mut self, query: &Query) {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let cte_name = cte.alias.name.value.clone();
                let cte_idx = self.add(cte_name);
                if let SetExpr::Select(s) = cte.query.body.as_ref() {
                    for dep in self.tables(s) {
                        let dep_idx = self.add(dep);
                        self.graph.add_edge(dep_idx, cte_idx, ());
                    }
                }
            }
        }
        if let SetExpr::Select(s) = query.body.as_ref() {
            let result = self.add("result".to_string());
            for dep in self.tables(s) {
                let dep_idx = self.add(dep);
                self.graph.add_edge(dep_idx, result, ());
            }
        }
    }

    fn tables(&self, select: &Select) -> Vec<String> {
        let mut tables = Vec::new();
        for t in &select.from {
            if let TableFactor::Table { name, alias, .. } = &t.relation {
                tables.push(alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| format!("{}", name)));
            }
            for j in &t.joins {
                if let TableFactor::Table { name, alias, .. } = &j.relation {
                    tables.push(alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| format!("{}", name)));
                }
            }
        }
        tables
    }
}
