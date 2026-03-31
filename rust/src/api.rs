use flutter_rust_bridge::frb;
use taskchampion::{
    chrono::{DateTime, Utc},
    Operations, Replica, ServerConfig, StorageConfig, Tag, Task,
};
use uuid::Uuid;
use std::{collections::HashMap, path::PathBuf, str::FromStr};
use serde_json;

fn parse_datetime(input: &str) -> Option<DateTime<Utc>> {
    if input.trim().is_empty() {
        return None;
    }
    input.parse::<DateTime<Utc>>().ok()
}

#[frb]
pub fn get_all_tasks_json(taskdb_dir_path: String) -> Result<String, taskchampion::Error> {
    let tasks = get_all_tasks(taskdb_dir_path); // your Vec<HashMap<String, String>>
    let json = serde_json::to_string(&tasks)
        .map_err(|e| taskchampion::Error::Other(anyhow::anyhow!(e)))?;
    Ok(json)
}

fn task_row_from_task(task: &Task) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    let mut tags = String::new();

    for (k, v) in task.get_taskmap() {
        if k.contains("tag_") {
            if let Some(stripped) = k.strip_prefix("tag_") {
                tags.push_str(stripped);
                tags.push(' ');
            }
        } else {
            map.insert(k.into(), v.into());
        }
    }
    map.insert("tags".into(), tags.trim().into());
    map.insert("uuid".into(), task.get_uuid().to_string());
    if !map.contains_key("status") {
        map.insert(
            "status".into(),
            task.get_value("status").unwrap_or("pending").to_string(),
        );
    }
    map
}

fn get_all_tasks(taskdb_dir_path: String) -> Vec<HashMap<String, String>> {
    let taskdb_dir = PathBuf::from(taskdb_dir_path);
    let storage = StorageConfig::OnDisk {
        taskdb_dir,
        create_if_missing: true,
        access_mode: taskchampion::storage::AccessMode::ReadWrite,
    }
    .into_storage()
    .unwrap();

    let mut replica = Replica::new(storage);
    let mut vector: Vec<HashMap<String, String>> = Vec::new();

    for (_, task) in replica.all_tasks().unwrap() {
        vector.push(task_row_from_task(&task));
    }
    vector
}

/// Optional filter keys (all omitted or empty = return all tasks):
/// - `uuid`: substring match on UUID (case-insensitive)
/// - `status`: `pending`, `completed`, or `deleted` (case-insensitive)
/// - `project`: exact match on project UDA
/// - `tags`: space-separated; `+tag` or bare `tag` means required; `-tag` means excluded
#[frb]
pub fn query_task(
    taskdb_dir_path: String,
    filter: HashMap<String, String>,
) -> Result<String, taskchampion::Error> {
    let taskdb_dir = PathBuf::from(taskdb_dir_path);
    let storage = StorageConfig::OnDisk {
        taskdb_dir,
        create_if_missing: true,
        access_mode: taskchampion::storage::AccessMode::ReadWrite,
    }
    .into_storage()?;

    let mut replica = Replica::new(storage);
    let all = replica.all_tasks()?;
    let mut out: Vec<HashMap<String, String>> = Vec::new();

    for (_, task) in all.iter() {
        let row = task_row_from_task(task);
        if filter.is_empty() || task_matches_filter(task, &row, &filter) {
            out.push(row);
        }
    }

    let json = serde_json::to_string(&out)
        .map_err(|e| taskchampion::Error::Other(anyhow::anyhow!(e)))?;
    Ok(json)
}

fn parse_tag_filter(spec: &str) -> (Vec<String>, Vec<String>) {
    let mut required = Vec::new();
    let mut excluded = Vec::new();
    for part in spec.split_whitespace() {
        if part.starts_with('+') && part.len() > 1 {
            required.push(part[1..].to_string());
        } else if part.starts_with('-') && part.len() > 1 {
            excluded.push(part[1..].to_string());
        } else if !part.is_empty() {
            required.push(part.to_string());
        }
    }
    (required, excluded)
}

fn task_matches_filter(
    task: &Task,
    row: &HashMap<String, String>,
    filter: &HashMap<String, String>,
) -> bool {
    if let Some(u) = filter.get("uuid") {
        if !u.is_empty() {
            let uuid = row.get("uuid").map(|s| s.as_str()).unwrap_or("");
            if !uuid.to_lowercase().contains(&u.to_lowercase()) {
                return false;
            }
        }
    }
    if let Some(s) = filter.get("status") {
        if !s.is_empty() {
            let got = row
                .get("status")
                .map(|x| x.to_lowercase())
                .unwrap_or_else(|| "pending".to_string());
            if got != s.to_lowercase() {
                return false;
            }
        }
    }
    if let Some(p) = filter.get("project") {
        let proj = task.get_user_defined_attribute("project").unwrap_or("");
        if p != proj {
            return false;
        }
    }
    if let Some(tag_spec) = filter.get("tags") {
        if !tag_spec.trim().is_empty() {
            let (required, excluded) = parse_tag_filter(tag_spec);
            for r in required {
                match Tag::from_str(&r) {
                    Ok(t) => {
                        if !task.has_tag(&t) {
                            return false;
                        }
                    }
                    Err(_) => return false,
                }
            }
            for e in excluded {
                match Tag::from_str(&e) {
                    Ok(t) => {
                        if task.has_tag(&t) {
                            return false;
                        }
                    }
                    Err(_) => return false,
                }
            }
        }
    }
    true
}

#[frb]
pub fn delete_task(uuid_st: String, taskdb_dir_path: String) -> i8 {
    let taskdb_dir = PathBuf::from(taskdb_dir_path);
    let storage = StorageConfig::OnDisk {
        taskdb_dir,
        create_if_missing: true,
        access_mode: taskchampion::storage::AccessMode::ReadWrite,
    }
    .into_storage()
    .unwrap();

    let mut replica = Replica::new(storage);
    let mut ops = Operations::new();
    let uuid = Uuid::parse_str(&uuid_st).unwrap();

    if let Some(mut t) = replica.get_task_data(uuid).unwrap() {
        t.delete(&mut ops);
    }
    replica.commit_operations(ops).unwrap();
    0
}

#[frb]
pub fn update_task(
    uuid_st: String,
    taskdb_dir_path: String,
    map: HashMap<String, String>,
) -> i8 {
    let taskdb_dir = PathBuf::from(taskdb_dir_path);
    let storage = StorageConfig::OnDisk {
        taskdb_dir,
        create_if_missing: true,
        access_mode: taskchampion::storage::AccessMode::ReadWrite,
    }
    .into_storage()
    .unwrap();

    let mut replica = Replica::new(storage);
    let mut ops = Operations::new();
    let uuid = Uuid::parse_str(&uuid_st).unwrap();

    if let Some(mut t) = replica.get_task(uuid).unwrap() {
        let _ = t.set_status(taskchampion::Status::Pending, &mut ops);
        for (key, value) in map {
            match key.as_str() {
                "description" => {
                    let _ = t.set_description(value, &mut ops);
                }
                "due" => {
                    let _ = t.set_due(parse_datetime(&value), &mut ops);
                }
                "start" => {
                    if value == "stop" {
                        let _ = t.stop(&mut ops);
                    } else {
                        let _ = t.start(&mut ops);
                    }
                }
                "wait" => {
                    let _ = t.set_wait(parse_datetime(&value), &mut ops);
                }
                "priority" => {
                    let _ = t.set_priority(value, &mut ops);
                }
                "tags" => {
					let existing_tags: Vec<String> = t
						.get_taskmap()
						.iter()
						.filter_map(|(k, _)| k.strip_prefix("tag_").map(|s| s.to_string()))
						.collect();
					for tag_name in existing_tags {
						println!("removing tag at rust side {}", tag_name);
						let mut tag = Tag::from_str(&tag_name).unwrap();
						let _ = t.remove_tag(&mut tag, &mut ops);
					}
                    
					for part in value.split_whitespace() {
                        println!("tag at rust side {}", part);
                        let mut tag = Tag::from_str(part).unwrap();
                        let _ = t.add_tag(&mut tag, &mut ops);
                    }
                }
                "project" => {
                    let _ = t.set_value("project", Some(value), &mut ops);
                }
                "status" => {
                    let status = match value.as_str() {
                        "pending" => taskchampion::Status::Pending,
                        "completed" => taskchampion::Status::Completed,
                        "deleted" => taskchampion::Status::Deleted,
                        _ => taskchampion::Status::Pending,
                    };
                    // print!("status at rust side {}", value);
                    println!("status at rust side {}", value);
                    let _ = t.set_status(status, &mut ops);
                }
                _ => {}
            }
        }
        replica.commit_operations(ops).unwrap();
    }
    0
}

#[frb]
pub fn add_task(taskdb_dir_path: String, map: HashMap<String, String>) -> i8 {
    let taskdb_dir = PathBuf::from(taskdb_dir_path);
    let storage = StorageConfig::OnDisk {
        taskdb_dir,
        create_if_missing: true,
        access_mode: taskchampion::storage::AccessMode::ReadWrite,
    }
    .into_storage()
    .unwrap();

    let mut replica = Replica::new(storage);
    let mut ops = Operations::new();
    if let Some(uuid_str) = map.get("uuid") {
    let uuid = Uuid::parse_str(&uuid_str).unwrap();
    let mut t = replica.create_task(uuid, &mut ops).unwrap();

    let _ = t.set_status(taskchampion::Status::Pending, &mut ops);

    for (key, value) in map {
        match key.as_str() {
            "description" => {
                let _ = t.set_description(value, &mut ops);
            }
            "due" => {
                let _ = t.set_due(parse_datetime(&value), &mut ops);
            }
            "start" => {
                let _ = t.start(&mut ops);
            }
            "wait" => {
                let _ = t.set_wait(parse_datetime(&value), &mut ops);
            }
            "priority" => {
                let _ = t.set_priority(value, &mut ops);
            }
            "tags" => {
                for part in value.split_whitespace() {
                    let mut tag = Tag::from_str(part).unwrap();
                    let _ = t.add_tag(&mut tag, &mut ops);
                }
            }
            "project" => {
                let _ = t.set_user_defined_attribute("project", value, &mut ops);
            }
            _ => {}
        }
    }
    replica.commit_operations(ops).unwrap();
    return 0;
    }
    1
}

#[frb]
pub async fn sync(
    taskdb_dir_path: String,
    url: String,
    client_id: String,
    encryption_secret: String,
) -> i8 {
    let taskdb_dir = PathBuf::from(taskdb_dir_path);
    let storage = StorageConfig::OnDisk {
        taskdb_dir,
        create_if_missing: true,
        access_mode: taskchampion::storage::AccessMode::ReadWrite,
    }
    .into_storage()
    .unwrap();

    let mut replica = Replica::new(storage);
    let config = ServerConfig::Remote {
        url: url.into(),
        client_id: Uuid::parse_str(&client_id).unwrap(),
        encryption_secret: encryption_secret.into(),
    };

    let mut server = config.into_server().unwrap();
    replica.sync(&mut server, false).unwrap();
    0
}

#[test]
fn test_add_task_with_tags() {
    use std::{collections::HashMap, env, fs};
    // create unique temporary directory for taskdb
    let tmp = env::temp_dir().join(format!("taskdb_test_{}", Uuid::new_v4()));
    let taskdb_path = tmp.to_string_lossy().into_owned();
    fs::create_dir_all(&tmp).expect("create temp taskdb dir");

    // prepare task map with tags
    let mut map: HashMap<String, String> = HashMap::new();
    let uuid = Uuid::new_v4().to_string();
    map.insert("uuid".to_string(), uuid.clone());
    map.insert("description".to_string(), "test task".to_string());
    map.insert("tags".to_string(), "tag1 tag2".to_string());

    // add task
    let res = add_task(taskdb_path.clone(), map);
    assert_eq!(res, 0);

    // read tasks as json and verify tags are present
    let json = get_all_tasks_json(taskdb_path.clone()).expect("get_all_tasks_json");
    let tasks: Vec<HashMap<String, String>> = serde_json::from_str(&json).expect("parse json");
    let found = tasks.into_iter().find(|m| m.get("uuid").map(|s| s == &uuid).unwrap_or(false));
    assert!(found.is_some(), "task with uuid not found");
    let task = found.unwrap();
    let tags = task.get("tags").map(|s| s.as_str()).unwrap_or("");
    assert!(tags.contains("tag1"), "tag1 missing in tags: {}", tags);
    assert!(tags.contains("tag2"), "tag2 missing in tags: {}", tags);

    // cleanup
    fs::remove_dir_all(&tmp).ok();
}

#[test]
fn test_query_task_by_status_and_project_and_tags() {
    use std::{collections::HashMap, env, fs};
    let tmp = env::temp_dir().join(format!("taskdb_query_{}", Uuid::new_v4()));
    let taskdb_path = tmp.to_string_lossy().into_owned();
    fs::create_dir_all(&tmp).expect("create temp taskdb dir");

    let mut a: HashMap<String, String> = HashMap::new();
    let ua = Uuid::new_v4().to_string();
    a.insert("uuid".to_string(), ua.clone());
    a.insert("description".to_string(), "alpha".to_string());
    a.insert("tags".to_string(), "work home".to_string());
    a.insert("project".to_string(), "projA".to_string());
    assert_eq!(add_task(taskdb_path.clone(), a), 0);

    let mut b: HashMap<String, String> = HashMap::new();
    let ub = Uuid::new_v4().to_string();
    b.insert("uuid".to_string(), ub.clone());
    b.insert("description".to_string(), "beta".to_string());
    b.insert("tags".to_string(), "work".to_string());
    b.insert("project".to_string(), "projB".to_string());
    assert_eq!(add_task(taskdb_path.clone(), b), 0);

    let pending: HashMap<String, String> =
        [("status".to_string(), "pending".to_string())]
            .into_iter()
            .collect();
    let json = query_task(taskdb_path.clone(), pending).expect("query_task");
    let tasks: Vec<HashMap<String, String>> = serde_json::from_str(&json).unwrap();
    assert_eq!(tasks.len(), 2);

    let mut f_proj = HashMap::new();
    f_proj.insert("project".to_string(), "projA".to_string());
    let json = query_task(taskdb_path.clone(), f_proj).expect("query project");
    let tasks: Vec<HashMap<String, String>> = serde_json::from_str(&json).unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].get("uuid").map(|s| s.as_str()), Some(ua.as_str()));

    let mut f_tags = HashMap::new();
    f_tags.insert("tags".to_string(), "+work -home".to_string());
    let json = query_task(taskdb_path.clone(), f_tags).expect("query tags");
    let tasks: Vec<HashMap<String, String>> = serde_json::from_str(&json).unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].get("uuid").map(|s| s.as_str()), Some(ub.as_str()));

    let mut f_uuid = HashMap::new();
    f_uuid.insert("uuid".to_string(), ub[..8].to_string());
    let json = query_task(taskdb_path.clone(), f_uuid).expect("query uuid");
    let tasks: Vec<HashMap<String, String>> = serde_json::from_str(&json).unwrap();
    assert_eq!(tasks.len(), 1);

    fs::remove_dir_all(&tmp).ok();
}
