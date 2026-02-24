import os
import re
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
import requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_OKR_DATABASE_ID = os.environ["NOTION_OKR_DATABASE_ID"]
LINEAR_API_KEY = os.environ["LINEAR_API_KEY"]

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"
LINEAR_BASE = "https://api.linear.app/graphql"

HEADER = "Weekly Linear update log"

def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def linear_headers():
    return {
        "Authorization": LINEAR_API_KEY,
        "Content-Type": "application/json",
    }

def linear_slug_from_project_url(url: str) -> Optional[str]:
    # Example: https://linear.app/tinyfish/project/authentication-workflows-9cb6b72850e3
    m = re.search(r"/project/([^/?#]+)", url or "")
    return m.group(1) if m else None

def linear_graphql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(
        LINEAR_BASE,
        headers=linear_headers(),
        json={"query": query, "variables": variables},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(data["errors"])
    return data["data"]

def notion_db_query(database_id: str) -> List[Dict[str, Any]]:
    # Filter: Linear project URL is not empty
    url = f"{NOTION_BASE}/databases/{database_id}/query"
    payload = {
        "filter": {
            "property": "Linear project URL",
            "url": {
                "is_not_empty": True
            }
        }
    }
    results = []
    next_cursor = None
    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        r = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
        r.raise_for_status()
        j = r.json()
        results.extend(j["results"])
        if not j.get("has_more"):
            break
        next_cursor = j.get("next_cursor")
    return results

def notion_get_page_markdown(page_id: str) -> str:
    """
    Practical approach:
    - Use the Notion API blocks endpoint and reconstruct text.
    - For simplicity here, we’ll just append blocks via API rather than doing a full markdown roundtrip.
    """
    raise NotImplementedError("See note below: use blocks API to append content under a heading.")

def notion_update_page_property(page_id: str, latest_update: str) -> None:
    url = f"{NOTION_BASE}/pages/{page_id}"
    payload = {
        "properties": {
            "Latest update": {
                "rich_text": [{"text": {"content": latest_update[:2000]}}]
            }
        }
    }
    r = requests.patch(url, headers=notion_headers(), json=payload, timeout=30)
    r.raise_for_status()

def notion_append_weekly_log_blocks(page_id: str, exec_update: str, project_name: str) -> None:
    """
    Append-only: add a new bulleted list item under the heading `### Weekly Linear update log`.
    Implementation strategy (recommended):
      1) Find or create the heading block on the page.
      2) Append a bulleted list item as a child under that heading.
    """
    # 1) List top-level blocks
    blocks_url = f"{NOTION_BASE}/blocks/{page_id}/children?page_size=100"
    blocks = []
    next_cursor = None
    while True:
        u = blocks_url + (f"&start_cursor={next_cursor}" if next_cursor else "")
        r = requests.get(u, headers=notion_headers(), timeout=30)
        r.raise_for_status()
        j = r.json()
        blocks.extend(j["results"])
        if not j.get("has_more"):
            break
        next_cursor = j.get("next_cursor")

    # 2) Find heading_3 with text == HEADER
    heading_block_id = None
    for b in blocks:
        if b.get("type") == "heading_3":
            rt = b["heading_3"].get("rich_text", [])
            text = "".join([x.get("plain_text", "") for x in rt]).strip()
            if text == HEADER:
                heading_block_id = b["id"]
                break

    # 3) Create heading if missing
    if heading_block_id is None:
        create_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "heading_3",
                    "heading_3": {"rich_text": [{"type": "text", "text": {"content": HEADER}}]}
                }
            ]
        }
        r = requests.patch(f"{NOTION_BASE}/blocks/{page_id}/children", headers=notion_headers(), json=create_payload, timeout=30)
        r.raise_for_status()
        # refresh blocks to get the new heading id
        r2 = requests.get(f"{NOTION_BASE}/blocks/{page_id}/children?page_size=100", headers=notion_headers(), timeout=30)
        r2.raise_for_status()
        for b in r2.json()["results"]:
            if b.get("type") == "heading_3":
                rt = b["heading_3"].get("rich_text", [])
                text = "".join([x.get("plain_text", "") for x in rt]).strip()
                if text == HEADER:
                    heading_block_id = b["id"]
                    break

    if heading_block_id is None:
        raise RuntimeError("Could not create/find Weekly Linear update log heading")

    # 4) Append bullet as child blocks under the heading
    today = dt.date.today().isoformat()
    bullet_title = f"{today} — {project_name}"

    # Break exec_update into sub-bullets (simple: split by lines that start with "- " or use fixed sections)
    sublines = [line.strip() for line in exec_update.splitlines() if line.strip()]

    children = [
        {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [{"type": "text", "text": {"content": bullet_title}}],
                "children": [
                    {
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{"type": "text", "text": {"content": line[:2000]}}]
                        }
                    }
                    for line in sublines[:20]  # safety cap
                ],
            },
        }
    ]

    payload = {"children": children}
    r = requests.patch(f"{NOTION_BASE}/blocks/{heading_block_id}/children", headers=notion_headers(), json=payload, timeout=30)
    r.raise_for_status()

PROJECT_QUERY = """
query ProjectBySlug($slug: String!) {
  projects(filter: { slug: { eq: $slug } }) {
    nodes {
      id
      name
      lead { name }
      state
      health
      updatedAt
      projectUpdates(first: 1, orderBy: createdAt, orderDirection: DESC) {
        nodes { body createdAt user { name } }
      }
    }
  }
}
"""

ISSUES_QUERY = """
query IssuesByProject($projectId: String!, $after: String) {
  issues(
    filter: { project: { id: { eq: $projectId } } }
    first: 250
    after: $after
    orderBy: updatedAt
  ) {
    nodes {
      title
      url
      updatedAt
      state { name type }  # type often maps to started/completed/canceled; varies by workspace config
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def fetch_project_and_issues(project_slug: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    pdata = linear_graphql(PROJECT_QUERY, {"slug": project_slug})
    nodes = pdata["projects"]["nodes"]
    if not nodes:
        raise RuntimeError(f"No Linear project found for slug: {project_slug}")
    project = nodes[0]

    issues = []
    after = None
    while True:
        idata = linear_graphql(ISSUES_QUERY, {"projectId": project["id"], "after": after})
        chunk = idata["issues"]["nodes"]
        issues.extend(chunk)
        page = idata["issues"]["pageInfo"]
        if not page["hasNextPage"]:
            break
        after = page["endCursor"]
        if len(issues) > 2000:
            break  # safety cap

    return project, issues

def bucket_issue(state_type: str, state_name: str) -> str:
    # Linear "state.type" commonly: started, completed, canceled, backlog, unstarted
    # We'll map to the buckets you asked for.
    st = (state_type or "").lower()
    name = (state_name or "").lower()

    if st == "completed" or name in {"done", "completed"}:
        return "done"
    if "review" in name:
        return "in_review"
    if st == "started" or name in {"in progress", "in-progress"}:
        return "in_progress"
    # default
    return "other"

def top_titles(issues: List[Dict[str, Any]], n: int = 3) -> List[str]:
    # issues already pulled orderBy updatedAt DESC, but sort defensively
    issues_sorted = sorted(issues, key=lambda x: x.get("updatedAt", ""), reverse=True)
    return [i["title"] for i in issues_sorted[:n]]

def format_exec_update(project: Dict[str, Any], issues: List[Dict[str, Any]]) -> str:
    # Most recent project update
    updates = project.get("projectUpdates", {}).get("nodes", [])
    if updates:
        u = updates[0]
        recent = f"Most recent update: {u['user']['name']} ({u['createdAt']}): {u['body'][:400].replace('\\n', ' ')}"
    else:
        recent = "Most recent update: No project update found in Linear."

    done = []
    in_review = []
    in_progress = []
    for it in issues:
        bucket = bucket_issue(it["state"].get("type"), it["state"].get("name"))
        if bucket == "done":
            done.append(it)
        elif bucket == "in_review":
            in_review.append(it)
        elif bucket == "in_progress":
            in_progress.append(it)

    done_titles = top_titles(done)
    review_titles = top_titles(in_review)
    prog_titles = top_titles(in_progress)

    lines = []
    lines.append(recent)

    lines.append(f"Most recently completed (counts): Done={len(done)}; In Review={len(in_review)}")
    if done_titles:
        lines.append("Top Done titles: " + "; ".join(done_titles))
    if review_titles:
        lines.append("Top In Review titles: " + "; ".join(review_titles))

    lines.append(f"Current ongoing work (counts): In Progress={len(in_progress)}")
    if prog_titles:
        lines.append("Top In Progress titles: " + "; ".join(prog_titles))

    lines.append(f"Leadership readout: status={project.get('state')}; health={project.get('health')}")

    return "\n".join(lines)

def main():
    pages = notion_db_query(NOTION_OKR_DATABASE_ID)
    print(f"Found {len(pages)} OKR rows with Linear project URLs")

    for p in pages:
        page_id = p["id"]
        props = p["properties"]

        linear_url = props["Linear project URL"]["url"]
        slug = linear_slug_from_project_url(linear_url)
        if not slug:
            print(f"Skipping page {page_id}: cannot parse Linear slug from {linear_url}")
            continue

        objective = "".join([t["plain_text"] for t in props["Objective"]["title"]]).strip()

        project, issues = fetch_project_and_issues(slug)
        exec_update = format_exec_update(project, issues)

        # 1) Overwrite Latest update
        notion_update_page_property(page_id, exec_update)

        # 2) Append to Weekly Linear update log in page body (append-only)
        notion_append_weekly_log_blocks(page_id, exec_update, project.get("name") or objective)

        print(f"Updated: {objective} ({slug})")

if __name__ == "__main__":
    main()
