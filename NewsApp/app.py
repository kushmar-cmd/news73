import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
import requests
import threading
import time

app = Flask(__name__)

FEEDS = {
    "עולם": [
        ("BBC World", "http://feeds.bbci.co.uk/news/world/rss.xml"),
        ("Reuters World", "https://feeds.reuters.com/reuters/worldNews"),
        ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    ],
    "טכנולוגיה": [
        ("TechCrunch", "https://techcrunch.com/feed/"),
        ("Ars Technica", "http://feeds.arstechnica.com/arstechnica/index"),
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
        ("Wired", "https://www.wired.com/feed/rss"),
    ],
    "מדע": [
        ("Science Daily", "https://www.sciencedaily.com/rss/all.xml"),
        ("NASA", "https://www.nasa.gov/rss/dyn/breaking_news.rss"),
        ("New Scientist", "https://www.newscientist.com/feed/home/"),
    ],
    "עסקים וכלכלה": [
        ("BBC Business", "http://feeds.bbci.co.uk/news/business/rss.xml"),
        ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
        ("Financial Times", "https://www.ft.com/rss/home/uk"),
    ],
    "ספורט": [
        ("BBC Sport", "http://feeds.bbci.co.uk/sport/rss.xml"),
        ("ESPN", "https://www.espn.com/espn/rss/news"),
        ("Sky Sports", "https://www.skysports.com/rss/12040"),
    ],
    "בריאות": [
        ("BBC Health", "http://feeds.bbci.co.uk/news/health/rss.xml"),
        ("WebMD", "https://rss.webmd.com/rss/rss.aspx?RSSSource=RSS_PUBLIC"),
        ("WHO", "https://www.who.int/rss-feeds/news-releases.xml"),
    ],
    "בידור": [
        ("BBC Entertainment", "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml"),
        ("Rolling Stone", "https://www.rollingstone.com/feed/"),
        ("Hollywood Reporter", "https://www.hollywoodreporter.com/feed/"),
    ],
    "מדיניות ופוליטיקה": [
        ("BBC Politics", "http://feeds.bbci.co.uk/news/politics/rss.xml"),
        ("Reuters Politics", "https://feeds.reuters.com/Reuters/PoliticsNews"),
        ("Politico", "https://www.politico.com/rss/politics08.xml"),
    ],
    "סביבה ואקלים": [
        ("BBC Environment", "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml"),
        ("Guardian Environment", "https://www.theguardian.com/environment/rss"),
        ("Climate Home News", "https://www.climatechangenews.com/feed/"),
    ],
    "חינוך": [
        ("BBC Education", "http://feeds.bbci.co.uk/news/education/rss.xml"),
        ("Times Higher Education", "https://www.timeshighereducation.com/news/rss.xml"),
        ("EdSurge", "https://www.edsurge.com/news.rss"),
    ],
}

cache = {}
cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes


def parse_rss(xml_text, source_name):
    items = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        # Standard RSS
        for item in root.findall(".//item")[:8]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            desc = item.findtext("description", "").strip()
            pub = item.findtext("pubDate", "").strip()
            if title and link:
                items.append({"title": title, "link": link, "desc": _clean(desc)[:200], "pub": pub, "source": source_name})
        # Atom feeds
        if not items:
            for entry in root.findall(".//atom:entry", ns)[:8]:
                title = entry.findtext("atom:title", "", ns).strip()
                link_el = entry.find("atom:link", ns)
                link = link_el.get("href", "") if link_el is not None else ""
                summary = entry.findtext("atom:summary", "", ns).strip()
                pub = entry.findtext("atom:updated", "", ns).strip()
                if title and link:
                    items.append({"title": title, "link": link, "desc": _clean(summary)[:200], "pub": pub, "source": source_name})
    except Exception:
        pass
    return items


def _clean(text):
    import re
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def fetch_category(category, feeds):
    articles = []
    for source_name, url in feeds:
        try:
            resp = requests.get(url, timeout=8, headers={"User-Agent": "NewsApp/1.0"})
            if resp.status_code == 200:
                articles.extend(parse_rss(resp.text, source_name))
        except Exception:
            pass
    return articles


def refresh_cache():
    while True:
        for category, feeds in FEEDS.items():
            articles = fetch_category(category, feeds)
            with cache_lock:
                cache[category] = {"articles": articles, "updated": datetime.now(timezone.utc).isoformat()}
        time.sleep(CACHE_TTL)


@app.route("/")
def index():
    return render_template("index.html", categories=list(FEEDS.keys()))


@app.route("/api/news")
def all_news():
    with cache_lock:
        return jsonify(dict(cache))


@app.route("/api/news/<category>")
def category_news(category):
    with cache_lock:
        data = cache.get(category)
    if not data:
        articles = fetch_category(category, FEEDS.get(category, []))
        data = {"articles": articles, "updated": datetime.now(timezone.utc).isoformat()}
        with cache_lock:
            cache[category] = data
    return jsonify(data)


if __name__ == "__main__":
    t = threading.Thread(target=refresh_cache, daemon=True)
    t.start()
    app.run(debug=False, port=5000)
