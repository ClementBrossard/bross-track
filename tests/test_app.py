import importlib


def _client(monkeypatch, tmp_path, password="secret"):
    monkeypatch.setenv("APP_PASSWORD", password)
    monkeypatch.setenv("SECRET_KEY", "test")
    monkeypatch.setenv("GALOPTRACK_STORAGE", "local")
    monkeypatch.setenv("GALOPTRACK_LOCAL_DIR", str(tmp_path))
    import app as app_module
    importlib.reload(app_module)
    return app_module.app.test_client()


def test_login_and_pages(monkeypatch, tmp_path):
    (tmp_path / "dashboards").mkdir()
    (tmp_path / "dashboards" / "latest.html").write_text("<html>DASH</html>")
    c = _client(monkeypatch, tmp_path)
    assert c.get("/healthz").data == b"ok"
    assert c.get("/dashboard").status_code == 302          # pas connecté -> login
    assert b"incorrect" in c.post("/login", data={"password": "faux"}).data
    r = c.post("/login", data={"password": "secret"})
    assert r.status_code == 302 and r.headers["Location"].endswith("/dashboard")
    assert c.get("/").headers["Location"].endswith("/dashboard")
    assert b"DASH" in c.get("/dashboard").data
    assert c.get("/sante").status_code == 503              # pas encore générée


def test_no_password_configured_blocks_login(monkeypatch, tmp_path):
    c = _client(monkeypatch, tmp_path, password="")
    assert b"incorrect" in c.post("/login", data={"password": ""}).data


def test_live_odds_api(monkeypatch, tmp_path):
    c = _client(monkeypatch, tmp_path)
    import app as app_module
    calls = []

    def fake(url):
        calls.append(url)
        if url.endswith("/participants"):
            return {"participants": [
                {"nom": "Alpha ", "numPmu": 1, "dernierRapportDirect": {"typePari": "SIMPLE_GAGNANT", "rapport": 3.5,
                                                                         "dateRapport": 1},
                 "dernierRapportReference": {"typePari": "SIMPLE_GAGNANT", "rapport": 5.0}},
                {"nom": "Beta", "numPmu": 2, "statut": "NON_PARTANT"},
            ]}
        return {"heureDepart": 1792929110000, "statut": "PROGRAMMEE"}

    monkeypatch.setattr(app_module, "_pmu_json", fake)
    assert c.get("/api/cotes/20261027/1/3").status_code == 401        # pas connecté
    c.post("/login", data={"password": "secret"})
    assert c.get("/api/cotes/2026-10-27/1/3").status_code == 400
    j = c.get("/api/cotes/20261027/1/3").get_json()
    assert calls[0].endswith("/27102026/R1/C3/participants")
    assert j["partants"][0] == {"num": 1, "nom": "ALPHA", "cote": 3.5, "cote_ref": 5.0, "heure_cote": 1,
                                "non_partant": False}
    assert j["partants"][1]["non_partant"] and j["partants"][1]["cote"] is None
    assert j["depart"] == 1792929110000
    n = len(calls)
    c.get("/api/cotes/20261027/1/3")                                   # servi par le cache
    assert len(calls) == n
