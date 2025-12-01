use std::collections::HashMap;

/// Represents the visual prefix for a single task in the graph
#[derive(Debug, Clone)]
pub struct GraphPrefix {
    pub prefix: String,  // The full tree prefix (e.g., "│   ├── ")
}

impl GraphPrefix {
    pub fn new(prefix: String) -> Self {
        GraphPrefix { prefix }
    }

    /// Render the graph prefix
    pub fn render(&self) -> String {
        self.prefix.clone()
    }
}

/// Build a tree-based graph layout showing task dependencies
/// 
/// This follows the Python visualize_tree.py approach: traverse from root tasks
/// and show each task as it appears in the tree. Tasks with multiple parents
/// will appear multiple times.
/// 
/// Returns a ORDERED list of (task_id, prefix) pairs in tree traversal order.
/// This means tasks appear in the order they would be printed in a tree view.
pub fn build_graph_layout_ordered(
    dependencies: &HashMap<String, Vec<String>>,
) -> Vec<(String, GraphPrefix)> {
    let mut result: Vec<(String, GraphPrefix)> = Vec::new();
    
    // Build downstream map: task -> list of tasks that depend on it (children)
    let mut downstream_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut all_tasks: Vec<String> = Vec::new();
    
    for (task_id, deps) in dependencies {
        all_tasks.push(task_id.clone());
        for dep in deps {
            downstream_map
                .entry(dep.clone())
                .or_insert_with(Vec::new)
                .push(task_id.clone());
        }
    }

    // Sort children alphabetically for consistent ordering
    for children in downstream_map.values_mut() {
        children.sort();
    }

    // Find root tasks (no dependencies)
    let mut root_tasks: Vec<String> = dependencies
        .iter()
        .filter(|(_, deps)| deps.is_empty())
        .map(|(task_id, _)| task_id.clone())
        .collect();
    
    // Also check for tasks not in dependencies map (orphaned tasks)
    for task_id in &all_tasks {
        if !dependencies.contains_key(task_id) {
            root_tasks.push(task_id.clone());
        }
    }
    
    root_tasks.sort();
    root_tasks.dedup();

    // Traverse tree and build ordered list
    for (root_idx, root_task) in root_tasks.iter().enumerate() {
        let is_last_root = root_idx == root_tasks.len() - 1;
        print_tree_recursive(
            root_task,
            &downstream_map,
            "",
            is_last_root,
            &mut result,
        );
    }

    result
}

/// Recursively traverse tree and record each task with its prefix
fn print_tree_recursive(
    task_id: &str,
    downstream_map: &HashMap<String, Vec<String>>,
    prefix: &str,
    is_last: bool,
    result: &mut Vec<(String, GraphPrefix)>,
) {
    // Build prefix for this task (using compact 2-space indentation)
    let connector = if is_last { "└─" } else { "├─" };
    let task_prefix = format!("{}{}", prefix, connector);
    
    // Record this task with its prefix
    result.push((task_id.to_string(), GraphPrefix::new(task_prefix)));

    // Get downstream tasks (children)
    if let Some(children) = downstream_map.get(task_id) {
        if !children.is_empty() {
            // Build prefix for children (2 spaces instead of 4)
            let extension = if is_last { "  " } else { "│ " };
            let child_prefix = format!("{}{}", prefix, extension);

            // Process each child
            for (i, child_id) in children.iter().enumerate() {
                let is_last_child = i == children.len() - 1;
                print_tree_recursive(
                    child_id,
                    downstream_map,
                    &child_prefix,
                    is_last_child,
                    result,
                );
            }
        }
    }
}

/// Build a graph layout as a HashMap for backward compatibility
/// Note: This will only show each task once (its first appearance in tree order)
pub fn build_graph_layout(
    sorted_tasks: &[String],
    dependencies: &HashMap<String, Vec<String>>,
) -> HashMap<String, GraphPrefix> {
    let ordered = build_graph_layout_ordered(dependencies);
    
    // Convert to HashMap, keeping first occurrence of each task
    let mut result: HashMap<String, GraphPrefix> = HashMap::new();
    for (task_id, prefix) in ordered {
        result.entry(task_id).or_insert(prefix);
    }
    
    // Ensure all sorted_tasks are in result (fallback for tasks not in tree)
    for task_id in sorted_tasks {
        result.entry(task_id.clone()).or_insert_with(|| GraphPrefix::new(String::new()));
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_dag() {
        // A -> B -> C
        let mut deps = HashMap::new();
        deps.insert("A".to_string(), vec![]);
        deps.insert("B".to_string(), vec!["A".to_string()]);
        deps.insert("C".to_string(), vec!["B".to_string()]);

        let ordered = build_graph_layout_ordered(&deps);
        
        println!("\nLinear DAG (tree order):");
        for (task_id, prefix) in &ordered {
            println!("{}◉ {}", prefix.render(), task_id);
        }
        
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].0, "A");
        assert_eq!(ordered[1].0, "B");
        assert_eq!(ordered[2].0, "C");
    }

    #[test]
    fn test_diamond_dag() {
        // start -> [task1, task2] -> end
        let mut deps = HashMap::new();
        deps.insert("start".to_string(), vec![]);
        deps.insert("task1".to_string(), vec!["start".to_string()]);
        deps.insert("task2".to_string(), vec!["start".to_string()]);
        deps.insert("end".to_string(), vec!["task1".to_string(), "task2".to_string()]);

        let ordered = build_graph_layout_ordered(&deps);
        
        println!("\nDiamond DAG (tree order):");
        for (task_id, prefix) in &ordered {
            println!("{}◉ {}", prefix.render(), task_id);
        }
        
        // Should show: start, task1, end, task2, end (end appears twice!)
        assert_eq!(ordered.len(), 5, "Should have 5 entries (end appears twice)");
        assert_eq!(ordered[0].0, "start");
        assert_eq!(ordered[1].0, "task1");
        assert_eq!(ordered[2].0, "end");
        assert_eq!(ordered[3].0, "task2");
        assert_eq!(ordered[4].0, "end");
    }

    #[test]
    fn test_parallel_chains() {
        // Two parallel chains:
        // task1A -> task2A -> task3A
        // task1B -> task2B -> task3B
        let mut deps = HashMap::new();
        deps.insert("task1A".to_string(), vec![]);
        deps.insert("task2A".to_string(), vec!["task1A".to_string()]);
        deps.insert("task3A".to_string(), vec!["task2A".to_string()]);
        deps.insert("task1B".to_string(), vec![]);
        deps.insert("task2B".to_string(), vec!["task1B".to_string()]);
        deps.insert("task3B".to_string(), vec!["task2B".to_string()]);
        
        let ordered = build_graph_layout_ordered(&deps);
        
        println!("\nParallel Chains (tree order):");
        for (task_id, prefix) in &ordered {
            println!("{}◉ {}", prefix.render(), task_id);
        }
        
        // Should show chains together: task1A, task2A, task3A, task1B, task2B, task3B
        assert_eq!(ordered.len(), 6);
        assert_eq!(ordered[0].0, "task1A");
        assert_eq!(ordered[1].0, "task2A");
        assert_eq!(ordered[2].0, "task3A");
        assert_eq!(ordered[3].0, "task1B");
        assert_eq!(ordered[4].0, "task2B");
        assert_eq!(ordered[5].0, "task3B");
    }

    #[test]
    fn test_parallel_groups_with_chains() {
        // start -> [group1.task1 -> group1.task2 -> group1.task3,
        //           group2.task1 -> group2.task2] -> end
        let mut deps = HashMap::new();
        deps.insert("start".to_string(), vec![]);
        deps.insert("group1.task1".to_string(), vec!["start".to_string()]);
        deps.insert("group1.task2".to_string(), vec!["group1.task1".to_string()]);
        deps.insert("group1.task3".to_string(), vec!["group1.task2".to_string()]);
        deps.insert("group2.task1".to_string(), vec!["start".to_string()]);
        deps.insert("group2.task2".to_string(), vec!["group2.task1".to_string()]);
        deps.insert("end".to_string(), vec![
            "group1.task3".to_string(),
            "group2.task2".to_string(),
        ]);
        
        let ordered = build_graph_layout_ordered(&deps);
        
        println!("\nParallel Groups with Chains (tree order):");
        for (task_id, prefix) in &ordered {
            println!("{}◉ {}", prefix.render(), task_id);
        }
        
        // end should appear twice (once under each group)
        let end_count = ordered.iter().filter(|(id, _)| id == "end").count();
        assert_eq!(end_count, 2, "end should appear twice");
    }
}
