import argparse
import textwrap
import weaviate

CLASS_NAME = "Movie"
RESULT_PROPERTIES = [
    "title",
    "release_date",
    "overview",
    "tagline",
    "director",
    "top_cast",
    "avg_rating",
    "rating_count",
]

def create_client(endpoint):
    return weaviate.Client(url=endpoint)

def format_overview(overview):
    if not overview:
        return "(no overview available)"
    snippet = overview.strip().replace("\n", " ")
    return textwrap.shorten(snippet, width=240, placeholder="...")

def search_movies(client, query, limit=10, filter_field=None):
    q = client.query.get(CLASS_NAME, RESULT_PROPERTIES).with_near_text({"concepts": [query]})
    if filter_field is not None and filter_field != "any":
        q = q.with_where({
            "path": [filter_field],
            "operator": "Equal",
            "valueText": query,
        })
    response = q.with_limit(limit).do()

    hits = response.get("data", {}).get("Get", {}).get(CLASS_NAME, [])
    return hits

def main(query, endpoint, limit, field):
    client = create_client(endpoint)
    results = search_movies(client, query, limit, filter_field=field)

    if not results:
        print("No results found. Try a different query.")
        return

    for index, hit in enumerate(results, start=1):
        title = hit.get("title") or "Unknown title"
        release_date = hit.get("release_date") or "Unknown date"
        director = hit.get("director") or "Unknown director"
        rating = hit.get("avg_rating")
        rating_count = hit.get("rating_count")
        overview = format_overview(hit.get("overview"))
        top_cast = hit.get("top_cast") or []

        print(f"{index}. {title} ({release_date})")
        print(f"   Director: {director}")
        if isinstance(top_cast, list) and top_cast:
            print(f"   Top cast: {', '.join(top_cast[:5])}")
        if rating is not None:
            print(f"   Avg rating: {rating:.2f} ({rating_count} ratings)")
        print(f"   Overview: {overview}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search loaded movie data in Weaviate.")
    parser.add_argument("query", help="Natural-language search query")
    parser.add_argument("--endpoint", default="http://localhost:8080", help="Weaviate endpoint URL")
    parser.add_argument("--limit", type=int, default=10, help="Number of results to return")
    parser.add_argument(
        "--field",
        choices=["any", "director", "top_cast"],
        default="any",
        help="Optional exact field filter for actor/director name searches",
    )
    args = parser.parse_args()
    main(args.query, args.endpoint, args.limit, args.field)
