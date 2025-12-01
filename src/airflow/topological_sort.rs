use std::collections::{HashMap, HashSet};

/// Extract task group prefix from `task_id` (part before first '.')
fn get_task_group(task_id: &str) -> &str {
    task_id.split('.').next().unwrap_or(task_id)
}

/// Make downstream tasks available if all their upstream dependencies are processed
fn make_downstream_available(
    task_id: &str,
    downstream_map: &HashMap<&str, &Vec<String>>,
    upstream_map: &HashMap<String, HashSet<String>>,
    processed: &HashSet<String>,
    available: &mut HashSet<String>,
) {
    if let Some(downstream_ids) = downstream_map.get(task_id) {
        for downstream_id in *downstream_ids {
            if !processed.contains(downstream_id) {
                if let Some(upstream_deps) = upstream_map.get(downstream_id) {
                    if upstream_deps.iter().all(|dep| processed.contains(dep)) {
                        available.insert(downstream_id.clone());
                    }
                }
            }
        }
    }
}

/// Performs topological sort on tasks using Kahn's algorithm (modified version like Airflow uses)
/// 
/// Input: Vec<(task_id, downstream_task_ids)> where downstream means tasks that depend on this one
/// Output: Vec<task_id> sorted so that dependencies come before dependents
/// 
/// Example: If task A -> B (A's downstream is B), then A comes before B in sorted output
/// 
/// Tasks are grouped by task group prefix - all tasks from one group are processed together
/// before moving to the next group, keeping visual organization clean.
pub fn topological_sort(tasks: Vec<(String, Vec<String>)>) -> Vec<String> {
    if tasks.is_empty() {
        return vec![];
    }
    
    // Build upstream mapping: task_id -> list of tasks that must run before it
    let mut upstream_map: HashMap<String, HashSet<String>> = HashMap::new();
    let mut all_task_ids: HashSet<String> = HashSet::new();
    
    // Build downstream mapping for O(1) lookups: task_id -> downstream_task_ids
    let downstream_map: HashMap<&str, &Vec<String>> = tasks
        .iter()
        .map(|(task_id, downstream)| (task_id.as_str(), downstream))
        .collect();
    
    // Initialize all tasks
    for (task_id, downstream_ids) in &tasks {
        all_task_ids.insert(task_id.clone());
        upstream_map.entry(task_id.clone()).or_insert_with(HashSet::new);
        
        for downstream_id in downstream_ids {
            all_task_ids.insert(downstream_id.clone());
            upstream_map
                .entry(downstream_id.clone())
                .or_insert_with(HashSet::new)
                .insert(task_id.clone());
        }
    }
    
    let mut sorted: Vec<String> = Vec::new();
    let mut processed: HashSet<String> = HashSet::new();
    let mut available: HashSet<String> = all_task_ids
        .iter()
        .filter(|task_id| upstream_map.get(*task_id).map_or(true, |deps| deps.is_empty()))
        .cloned()
        .collect();
    
    // Process tasks group by group
    while !available.is_empty() {
        // Group available tasks by their task group
        let mut by_group: HashMap<String, Vec<String>> = HashMap::new();
        for task_id in &available {
            let group = get_task_group(task_id).to_string();
            by_group.entry(group).or_insert_with(Vec::new).push(task_id.clone());
        }
        
        // Sort groups alphabetically
        let mut groups: Vec<String> = by_group.keys().cloned().collect();
        groups.sort();
        
        // Process one group completely before moving to the next
        for group in groups {
            let mut group_tasks = by_group.get(&group).unwrap().clone();
            group_tasks.sort();
            
            // Process all tasks from this group that are currently available
            for task_id in group_tasks {
                if !available.contains(&task_id) || processed.contains(&task_id) {
                    continue;
                }
                
                // Process this task
                sorted.push(task_id.clone());
                processed.insert(task_id.clone());
                available.remove(&task_id);
                
                // Make downstream tasks available if ready
                make_downstream_available(&task_id, &downstream_map, &upstream_map, &processed, &mut available);
                
                // After processing a task, check if more tasks from THIS group are now available
                // This keeps us processing the same group
                let newly_available: Vec<String> = available
                    .iter()
                    .filter(|t| get_task_group(t) == group)
                    .cloned()
                    .collect();
                
                for new_task in newly_available {
                    if !processed.contains(&new_task) {
                        sorted.push(new_task.clone());
                        processed.insert(new_task.clone());
                        available.remove(&new_task);
                        
                        // Add its downstream tasks
                        make_downstream_available(&new_task, &downstream_map, &upstream_map, &processed, &mut available);
                    }
                }
            }
            // Break after processing one group to re-evaluate available tasks
            // This ensures we complete one group before moving to the next
            break;
        }
    }
    
    
    // Add any remaining tasks (shouldn't happen in valid DAGs)
    for task_id in all_task_ids {
        if !processed.contains(&task_id) {
            sorted.push(task_id);
        }
    }
    
    sorted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_linear_dag() {
        // A -> B -> C
        let tasks = vec![
            ("A".to_string(), vec!["B".to_string()]),
            ("B".to_string(), vec!["C".to_string()]),
            ("C".to_string(), vec![]),
        ];
        
        let sorted = topological_sort(tasks);
        assert_eq!(sorted, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_diamond_dag() {
        // start -> [task1, task2] -> end
        let tasks = vec![
            ("start".to_string(), vec!["task1".to_string(), "task2".to_string()]),
            ("task1".to_string(), vec!["end".to_string()]),
            ("task2".to_string(), vec!["end".to_string()]),
            ("end".to_string(), vec![]),
        ];
        
        let sorted = topological_sort(tasks);
        println!("Sorted result: {:?}", sorted);
        assert_eq!(sorted[0], "start");
        assert_eq!(sorted[3], "end", "Expected 'end' at position 3, got: {:?}", sorted);
        // task1 and task2 can be in any order (both depend on start)
        assert!(sorted[1..3].contains(&"task1".to_string()));
        assert!(sorted[1..3].contains(&"task2".to_string()));
    }

    #[test]
    fn test_empty() {
        let tasks = vec![];
        let sorted = topological_sort(tasks);
        assert_eq!(sorted, Vec::<String>::new());
    }
    
    #[test]
    fn test_real_dag_subset() {
        // Simplified subset of E-3701-gcp_adhoc_store_pickup DAG
        // start -> check_source_data -> oracle_to_gcs -> truncate_gcs_to_bq -> end_flow -> end
        let tasks = vec![
            ("end".to_string(), vec![]),
            ("start".to_string(), vec!["check_source_data".to_string()]),
            ("check_source_data".to_string(), vec!["oracle_to_gcs".to_string()]),
            ("oracle_to_gcs".to_string(), vec!["truncate_gcs_to_bq".to_string()]),
            ("truncate_gcs_to_bq".to_string(), vec!["end_flow".to_string()]),
            ("end_flow".to_string(), vec!["end".to_string()]),
        ];
        
        let sorted = topological_sort(tasks);
        println!("Real DAG subset sorted: {:?}", sorted);
        
        // Verify order
        assert_eq!(sorted[0], "start", "start should be first");
        assert_eq!(sorted[sorted.len() - 1], "end", "end should be last");
        
        // Verify dependencies are respected
        let start_pos = sorted.iter().position(|t| t == "start").unwrap();
        let check_pos = sorted.iter().position(|t| t == "check_source_data").unwrap();
        let oracle_pos = sorted.iter().position(|t| t == "oracle_to_gcs").unwrap();
        let truncate_pos = sorted.iter().position(|t| t == "truncate_gcs_to_bq").unwrap();
        let end_flow_pos = sorted.iter().position(|t| t == "end_flow").unwrap();
        let end_pos = sorted.iter().position(|t| t == "end").unwrap();
        
        assert!(start_pos < check_pos, "start before check_source_data");
        assert!(check_pos < oracle_pos, "check_source_data before oracle_to_gcs");
        assert!(oracle_pos < truncate_pos, "oracle_to_gcs before truncate_gcs_to_bq");
        assert!(truncate_pos < end_flow_pos, "truncate_gcs_to_bq before end_flow");
        assert!(end_flow_pos < end_pos, "end_flow before end");
    }
    
    #[test]
    fn test_alphabetical_sorting_same_level() {
        // start -> [zebra, apple, middle] -> end
        // At the same level, tasks should be alphabetically sorted
        let tasks = vec![
            ("start".to_string(), vec!["zebra".to_string(), "apple".to_string(), "middle".to_string()]),
            ("zebra".to_string(), vec!["end".to_string()]),
            ("apple".to_string(), vec!["end".to_string()]),
            ("middle".to_string(), vec!["end".to_string()]),
            ("end".to_string(), vec![]),
        ];
        
        let sorted = topological_sort(tasks);
        println!("Alphabetical sort test: {:?}", sorted);
        
        assert_eq!(sorted[0], "start", "start should be first");
        assert_eq!(sorted[4], "end", "end should be last");
        
        // Tasks at the same level (zebra, apple, middle) should be alphabetically sorted
        assert_eq!(sorted[1], "apple", "apple should be first among same-level tasks");
        assert_eq!(sorted[2], "middle", "middle should be second among same-level tasks");
        assert_eq!(sorted[3], "zebra", "zebra should be third among same-level tasks");
    }
    
    #[test]
    fn test_task_group_sorting() {
        // Test that task groups are kept together
        // start -> [group_b.task1, group_b.task2, group_a.task1, group_a.task2] -> end
        let tasks = vec![
            ("start".to_string(), vec![
                "group_b.task1".to_string(), 
                "group_b.task2".to_string(), 
                "group_a.task1".to_string(), 
                "group_a.task2".to_string()
            ]),
            ("group_b.task1".to_string(), vec!["end".to_string()]),
            ("group_b.task2".to_string(), vec!["end".to_string()]),
            ("group_a.task1".to_string(), vec!["end".to_string()]),
            ("group_a.task2".to_string(), vec!["end".to_string()]),
            ("end".to_string(), vec![]),
        ];
        
        let sorted = topological_sort(tasks);
        println!("Task group sort test: {:?}", sorted);
        
        assert_eq!(sorted[0], "start");
        assert_eq!(sorted[5], "end");
        
        // group_a tasks should be together and before group_b tasks
        assert_eq!(sorted[1], "group_a.task1");
        assert_eq!(sorted[2], "group_a.task2");
        assert_eq!(sorted[3], "group_b.task1");
        assert_eq!(sorted[4], "group_b.task2");
    }
}
