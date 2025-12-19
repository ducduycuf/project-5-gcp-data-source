import time, random
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from playwright.sync_api import sync_playwright, TimeoutError

# ======================
# CONFIG
# ======================
MONGO_URI = "mongodb://35.187.241.9:27017"
DB_NAME = "glamiradb"

TASKS_COL = "task6_crawl_retry_vm1"
RESULTS_COL = "task6_retry_results"

BATCH_SIZE = 20
MAX_RETRY = 2
PAGES_PER_VM = 2

REACT_FIELDS = [
    "product_id", "name", "sku", "price",
    "category", "category_name",
    "store_code", "gender"
]

# ======================
def delay(success=True):
    if success:
        time.sleep(random.uniform(0.5, 1.2))
    else:
        time.sleep(random.uniform(2.0, 3.5))


def setup_route(context):
    context.route(
        "**/*",
        lambda route, request: (
            route.abort()
            if request.resource_type in ["image", "font", "media"]
            else route.continue_()
        )
    )

# ======================
def load_tasks():
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][TASKS_COL]
    tasks = list(col.find({}, {"_id": 0}))
    print(f"TOTAL RETRY TASKS: {len(tasks)}")
    return tasks


def save_batch(rows):
    if not rows:
        return
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][RESULTS_COL]

    ops = [
        UpdateOne(
            {"product_id": r["product_id"]},
            {"$set": r},
            upsert=True
        ) for r in rows
    ]
    col.bulk_write(ops, ordered=False)

# ======================
def crawl(page, task):
    pid = task["product_id"]
    url = task["url"]

    result = {
        "product_id": pid,
        "url": url,
        "status": "FAILED",
        "react_fields": None,
        "error": None,
        "retry": True,
        "crawled_at": datetime.utcnow(),
    }

    for attempt in range(1, MAX_RETRY + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15000)

            page.wait_for_function(
                "() => window.react_data !== undefined",
                timeout=5000
            )

            react_data = page.evaluate("() => window.react_data")
            if not react_data:
                raise Exception("react_data empty")

            result["react_fields"] = {
                k: react_data.get(k) for k in REACT_FIELDS
            }
            result["status"] = "OK"
            return result, True

        except TimeoutError:
            result["error"] = f"Timeout (attempt {attempt})"
        except Exception as e:
            result["error"] = str(e)

        delay(success=False)

    return result, False

# ======================
def main():
    tasks = load_tasks()
    if not tasks:
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="en-US",
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        setup_route(context)
        pages = [context.new_page() for _ in range(PAGES_PER_VM)]

        batch = []
        total = len(tasks)

        for idx, task in enumerate(tasks):
            page = pages[idx % PAGES_PER_VM]

            print(f"[RETRY] {idx+1}/{total} | {task['product_id']}")
            result, success = crawl(page, task)

            delay(success)
            batch.append(result)

            if len(batch) >= BATCH_SIZE:
                save_batch(batch)
                batch.clear()

        if batch:
            save_batch(batch)

        browser.close()

    print("RETRY FINISHED")

if __name__ == "__main__":
    main()