import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from deep_translator import GoogleTranslator
    TRANSLATE_AVAILABLE = True
except ImportError:
    TRANSLATE_AVAILABLE = False

app = Flask(__name__)

# Sources marked True will have titles/descriptions translated to Hebrew
FEEDS = {
    "ישראל": [
        ("Ynet חדשות", "https://www.ynet.co.il/Integration/StoryRss2.xml", False),
        ("Walla ישראל", "https://rss.walla.co.il/feed/1", False),
        ("כאן חדשות", "https://www.kan.org.il/rss/", False),
        ("מאקו N12", "https://www.mako.co.il/rss/news.xml", False),
        ("הארץ", "https://www.haaretz.co.il/cmlink/1.1660017", False),
        ("ישראל היום", "https://www.israelhayom.co.il/rss.xml", False),
    ],
    "פוליטיקה ישראלית": [
        ("Ynet פוליטי", "https://www.ynet.co.il/Integration/StoryRss2030.xml", False),
        ("BBC Politics", "http://feeds.bbci.co.uk/news/politics/rss.xml", True),
        ("Politico", "https://www.politico.com/rss/politics08.xml", True),
    ],
    "עולם": [
        ("Walla עולם", "https://rss.walla.co.il/feed/2", False),
        ("BBC World", "http://feeds.bbci.co.uk/news/world/rss.xml", True),
        ("Guardian World", "https://www.theguardian.com/world/rss", True),
        ("Reuters World", "https://feeds.reuters.com/reuters/worldNews", True),
        ("Ynet עולם", "https://www.ynet.co.il/Integration/StoryRss377.xml", False),
    ],
    "כלכלה": [
        ("Ynet כלכלה", "https://www.ynet.co.il/Integration/StoryRss3.xml", False),
        ("Walla כלכלה", "https://rss.walla.co.il/feed/3", False),
        ("גלובס כלכלה", "https://www.globes.co.il/rss/rss.aspx?f=502", False),
        ("גלובס שוק ההון", "https://www.globes.co.il/rss/rss.aspx?f=561", False),
        ("TheMarker", "https://www.themarker.com/rss/", False),
        ("BBC Business", "http://feeds.bbci.co.uk/news/business/rss.xml", True),
        ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews", True),
    ],
    "טכנולוגיה": [
        ("Ynet טכנולוגיה", "https://www.ynet.co.il/Integration/StoryRss542.xml", False),
        ("Walla טכנולוגיה", "https://rss.walla.co.il/feed/4", False),
        ("TechCrunch", "https://techcrunch.com/feed/", True),
        ("The Verge", "https://www.theverge.com/rss/index.xml", True),
        ("Wired", "https://www.wired.com/feed/rss", True),
    ],
    "ספורט": [
        ("Ynet ספורט", "https://www.ynet.co.il/Integration/StoryRss5.xml", False),
        ("Walla ספורט", "https://rss.walla.co.il/feed/7", False),
        ("ספורט 5", "https://www.sport5.co.il/rss.aspx", False),
        ("BBC Sport", "http://feeds.bbci.co.uk/sport/rss.xml", True),
        ("ESPN", "https://www.espn.com/espn/rss/news", True),
    ],
    "בידור ותרבות": [
        ("Ynet בידור", "https://www.ynet.co.il/Integration/StoryRss4.xml", False),
        ("BBC Entertainment", "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml", True),
        ("Rolling Stone", "https://www.rollingstone.com/feed/", True),
    ],
    "בריאות": [
        ("Ynet בריאות", "https://www.ynet.co.il/Integration/StoryRss3458.xml", False),
        ("BBC Health", "http://feeds.bbci.co.uk/news/health/rss.xml", True),
        ("WHO", "https://www.who.int/rss-feeds/news-releases.xml", True),
    ],
    "מדע וטבע": [
        ("Ynet מדע", "https://www.ynet.co.il/Integration/StoryRss3462.xml", False),
        ("Science Daily", "https://www.sciencedaily.com/rss/all.xml", True),
        ("NASA", "https://www.nasa.gov/rss/dyn/breaking_news.rss", True),
        ("New Scientist", "https://www.newscientist.com/feed/home/", True),
    ],
    "סביבה ואקלים": [
        ("BBC Environment", "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml", True),
        ("Guardian Environment", "https://www.theguardian.com/environment/rss", True),
        ("Reuters Environment", "https://feeds.reuters.com/reuters/environment", True),
        ("Yale Environment", "https://e360.yale.edu/feed", True),
    ],
    "חינוך": [
        ("כאן חינוך", "https://www.kan.org.il/rss/?catid=4", False),
        ("Ynet חינוך", "https://www.ynet.co.il/Integration/StoryRss3459.xml", False),
        ("BBC Education", "http://feeds.bbci.co.uk/news/education/rss.xml", True),
        ("Times Higher Education", "https://www.timeshighereducation.com/news/rss.xml", True),
        ("EdSurge", "https://www.edsurge.com/news.rss", True),
    ],
}

cache = {}
cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes

_translator = None
_translator_lock = threading.Lock()


def get_translator():
    global _translator
    if not TRANSLATE_AVAILABLE:
        return None
    with _translator_lock:
        if _translator is None:
            try:
                _translator = GoogleTranslator(source="auto", target="iw")
            except Exception:
                pass
    return _translator


def translate_text(text):
    if not text:
        return text
    t = get_translator()
    if not t:
        return text
    try:
        return t.translate(text[:500]) or text
    except Exception:
        return text


def parse_rss(xml_text, source_name, do_translate=False):
    items = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for item in root.findall(".//item")[:6]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            desc = _clean(item.findtext("description", "").strip())[:200]
            pub = item.findtext("pubDate", "").strip()
            if title and link:
                items.append({"title": title, "link": link, "desc": desc, "pub": pub,
                              "source": source_name, "translate": do_translate})
        if not items:
            for entry in root.findall(".//atom:entry", ns)[:6]:
                title = entry.findtext("atom:title", "", ns).strip()
                link_el = entry.find("atom:link", ns)
                link = link_el.get("href", "") if link_el is not None else ""
                summary = _clean(entry.findtext("atom:summary", "", ns).strip())[:200]
                pub = entry.findtext("atom:updated", "", ns).strip()
                if title and link:
                    items.append({"title": title, "link": link, "desc": summary, "pub": pub,
                                  "source": source_name, "translate": do_translate})
    except Exception:
        pass
    return items


def translate_articles(articles):
    """Translate titles of English articles in a background thread."""
    t = get_translator()
    if not t:
        return
    for a in articles:
        if a.get("translate"):
            a["title"] = translate_text(a["title"])
            a["translate"] = False


def _clean(text):
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def fetch_feed(source_name, url, do_translate):
    try:
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0 (compatible; NewsApp/1.0)"})
        if resp.status_code == 200:
            if resp.encoding and resp.encoding.lower() in ("iso-8859-1", "windows-1252"):
                resp.encoding = resp.apparent_encoding
            return parse_rss(resp.text, source_name, do_translate)
    except Exception:
        pass
    return []


def fetch_category(category, feeds):
    articles = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_feed, name, url, translate): name for name, url, translate in feeds}
        for f in as_completed(futures, timeout=15):
            try:
                articles.extend(f.result())
            except Exception:
                pass
    return articles


def refresh_cache():
    while True:
        def _fetch_one(cat_feeds):
            cat, feeds = cat_feeds
            articles = fetch_category(cat, feeds)
            # Store immediately without translation so UI loads fast
            with cache_lock:
                cache[cat] = {"articles": articles, "updated": datetime.now(timezone.utc).isoformat()}
            # Translate in background after storing
            translate_articles(articles)

        with ThreadPoolExecutor(max_workers=6) as ex:
            list(ex.map(_fetch_one, FEEDS.items()))
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
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
