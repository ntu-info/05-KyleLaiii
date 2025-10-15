# app.py
from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        return term

    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        x, y, z = map(int, coords.split("_"))
        return jsonify([x, y, z])

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def get_studies_from_term_without_b(term_a, term_b):
        term_a = term_a.replace("_", " ")
        term_b = term_b.replace("_", " ")
        engine = get_engine()
        with engine.connect() as conn:
            sql = text("""
                SELECT DISTINCT at1.study_id, m.journal, m.year, m.title, :term_a AS term_a
                FROM ns.annotations_terms AS at1
                JOIN ns.metadata AS m
                ON m.study_id = at1.study_id
                WHERE at1.term = :term_a
                AND NOT EXISTS (
                    SELECT 1
                    FROM ns.annotations_terms AS at2
                    WHERE at2.study_id = at1.study_id
                        AND at2.term = :term_b
                )
                ORDER BY at1.study_id
            """)
            rows = conn.execute(sql, {
                "term_a": "terms_abstract_tfidf__" + term_a,
                "term_b": "terms_abstract_tfidf__" + term_b
            }).mappings().all()

        return jsonify([{"study_id": r["study_id"], "term_a":r["term_a"], "journal": r["journal"], "year": r["year"], "title": r["title"]} for r in rows])
        
    # 在執行這段程式碼之前，我有在Postgresql中先將ns.coordinates轉換成ns.coordinates_xyz
    # 其中ns.coordinates_xyz的coords欄位是json格式的[x, y, z]
    @app.get("/dissociate/locations/<x1_y1_z1>/<x2_y2_z2>", endpoint="dissociate_coord")
    def get_studies_from_coord_without_b(x1_y1_z1, x2_y2_z2):
        # 1. 將座標字串轉成浮點數
        x1, y1, z1 = map(float, x1_y1_z1.split("_"))
        x2, y2, z2 = map(float, x2_y2_z2.split("_"))
        engine = get_engine()
        with engine.connect() as conn:
            # 2. SQL查詢，join metadata 取得 title
            sql = text("""
                SELECT DISTINCT c1.study_id, c1.coords AS coord, m.journal, m.year, m.title
                FROM ns.coordinates_xyz AS c1
                LEFT JOIN ns.metadata AS m ON c1.study_id = m.study_id
                WHERE c1.coords = jsonb_build_array(:x1, :y1, :z1)
                AND NOT EXISTS (
                    SELECT 1
                    FROM ns.coordinates_xyz AS c2
                    WHERE c2.study_id = c1.study_id
                        AND c2.coords = jsonb_build_array(:x2, :y2, :z2)
                )
            """)
            rows = conn.execute(sql, {"x1": x1, "y1": y1, "z1": z1, "x2": x2, "y2": y2, "z2": z2}).mappings().all()
        return jsonify([{"study_id": r["study_id"],"journal": r["journal"], "year": r["year"], "title": r["title"], "coord": r["coord"]} for r in rows])
    
    @app.get("/range/locations/<x1_y1_z1>", endpoint="coord_in_range_default")
    @app.get("/range/locations/<x1_y1_z1>/<float:rad>", endpoint="coord_in_range")
    @app.get("/range/locations/<x1_y1_z1>/<int:rad>", endpoint="coord_in_range_int")
    def get_studies_in_radius(x1_y1_z1:str,rad = 0.0):
        rad = float(rad)

        if rad < 0:
            return jsonify({"error": "r must be >= 0"}), 400
            # 解析座標
        try:
            x1, y1, z1 = map(float, x1_y1_z1.split("_"))
        except Exception:
            return jsonify({"error": "invalid coordinate format; expected x_y_z"}), 400

        engine = get_engine()
        with engine.connect() as conn:
            sql = text("""
                SELECT 
                    c.study_id, 
                    array_agg(
                    c.coords
                    ORDER BY (c.coords->>0)::double precision,
                             (c.coords->>1)::double precision,
                             (c.coords->>2)::double precision
                    ) AS coords_in_range,
                    m.journal, m.year, m.title
                FROM ns.coordinates_xyz AS c
                LEFT JOIN ns.metadata AS m ON c.study_id = m.study_id
                WHERE 
                    sqrt(
                        power((c.coords->>0)::float - :x1, 2) +
                        power((c.coords->>1)::float - :y1, 2) +
                        power((c.coords->>2)::float - :z1, 2)
                    ) <= :rad
                GROUP BY c.study_id, m.journal, m.year, m.title
            """)
            rows = conn.execute(sql, {"x1": x1, "y1": y1, "z1": z1, "rad": rad}).mappings().all()

        # 回傳 study_id、coords_in_range（list of [x, y, z]）、title
        return jsonify([
            {
                "radius_used": rad,   # 方便你驗證真的用到幾
                "results": [
                    {"study_id": row["study_id"], "coords": row["coords_in_range"], 
                     "journal": row["journal"], "year": row["year"], "title": row["title"]}
                    for row in rows
                ]
            }
        ])

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    # Select a few columns if they exist; otherwise select a generic subset
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app

# WSGI entry point (no __main__)
app = create_app()
