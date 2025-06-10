import os
import yaml
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Any, Set, Tuple

def extract_purposes(purposes: Dict[str, List[str]]) -> str:
    """Convert purposes dictionary to a string format, keeping only the purpose categories."""
    if not purposes:
        return ""
    return ", ".join(purposes.keys())

def build_adjacency_list(links: List[Dict]) -> Dict[str, List[Tuple[str, Dict]]]:
    """Build an adjacency list representation of the graph with purposes."""
    adj_list = defaultdict(list)
    for link in links:
        source = link.get('source', '')
        target = link.get('target', '')
        purposes = link.get('purposes', {})
        if source and target:
            adj_list[source].append((target, purposes))
    return adj_list

def find_leaf_nodes(links: List[Dict]) -> Set[str]:
    """Identify leaf nodes (nodes that don't have any outgoing edges)."""
    sources = set(link.get('source', '') for link in links)
    targets = set(link.get('target', '') for link in links)
    return targets - sources

def find_root_nodes(links: List[Dict]) -> Set[str]:
    """Identify root nodes (nodes that appear as sources but not as targets)."""
    sources = set(link.get('source', '') for link in links)
    targets = set(link.get('target', '') for link in links)
    return sources - targets

def get_all_paths_with_purposes(adj_list, start, end, path=None, purposes_accumulated=None, all_paths=None):
    """Find all paths between start and end nodes while accumulating purposes along the way."""
    if path is None:
        path = []
    if purposes_accumulated is None:
        purposes_accumulated = defaultdict(list)
    if all_paths is None:
        all_paths = []
    
    path = path + [start]
    
    if start == end:
        all_paths.append((path, dict(purposes_accumulated)))
        return
    
    for next_node, purposes in adj_list.get(start, []):
        if next_node not in path:
            for category in purposes.keys():
                purposes_accumulated[category].extend(purposes[category])
            get_all_paths_with_purposes(adj_list, next_node, end, path, purposes_accumulated, all_paths)
    
    return all_paths

def process_yaml_to_organized_data(yaml_content: str) -> pd.DataFrame:
    """Convert YAML content to a DataFrame with inherited purposes from root to leaf and text in the fourth column."""
    try:
        data = yaml.safe_load(yaml_content)
        if not isinstance(data, dict) or 'links' not in data:
            return pd.DataFrame()
        
        links = data['links']
        if not isinstance(links, list):
            return pd.DataFrame()
        
        adj_list = build_adjacency_list(links)
        leaf_nodes = find_leaf_nodes(links)
        root_nodes = find_root_nodes(links)
        
        rows = []
        for root in root_nodes:
            for leaf in leaf_nodes:
                paths_with_purposes = get_all_paths_with_purposes(adj_list, root, leaf)
                
                for path, inherited_purposes in paths_with_purposes:
                    # Extracting text for each link and combining them in the row
                    for link in links:
                        if link['source'] == root and link['target'] == leaf:
                            text_content = "\n".join(link.get('text', []))  # Joining text lines with newlines
                            rows.append({
                                'data_type': leaf,
                                'collector': root,
                                'purpose': extract_purposes(inherited_purposes),
                                'text': text_content  # Add text to the fourth column
                            })
        
        df = pd.DataFrame(rows)
        
        if not df.empty:
            df = df.sort_values(['data_type', 'collector']).reset_index(drop=True)
            df = df.drop_duplicates()
        
        return df

    except yaml.YAMLError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()



def process_all_yaml_files(input_folder: str, output_folder: str):
    """Process all 'graph-original.yml' files in each subdirectory of input_folder and save them as .csv in output_folder."""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    for app_folder in os.listdir(input_folder):
        app_path = os.path.join(input_folder, app_folder)
        
        if os.path.isdir(app_path):  # Ensure it's a folder
            print(f"Processing folder: {app_path}")  # Debugging line
            files_in_folder = os.listdir(app_path)
            print(f"Files in folder: {files_in_folder}")  # Debugging line
            for filename in files_in_folder:
                if filename == "graph-original.yml":  # Check if the file is 'graph-original.yml'
                    input_path = os.path.join(app_path, filename)
                    
                    # Use the app folder name as the CSV filename
                    output_path = os.path.join(output_folder, f"{app_folder}.csv")
                    
                    try:
                        with open(input_path, 'r', encoding='utf-8') as file:
                            yaml_content = file.read()
                        
                        df = process_yaml_to_organized_data(yaml_content)
                        
                        if not df.empty:
                            df.to_csv(output_path, index=False)
                            print(f"Processed {filename} successfully and saved as {output_path}!")
                        else:
                            print(f"No valid data in {filename}")
                    except Exception as e:
                        print(f"Error processing {filename}: {e}")



if __name__ == "__main__":
    input_folder = "output"  # Change this to the actual input folder containing app subdirectories
    output_folder = "processed_csv"  # Change this to the actual output folder
    process_all_yaml_files(input_folder, output_folder)
