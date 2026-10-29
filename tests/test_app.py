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


def _fake_pmu(calls):
    def fake(url, timeout=8):
        calls.append(url)
        if url.endswith("/participants"):
            return {"participants": [
                {"nom": "Alpha ", "numPmu": 1, "ordreArrivee": 2, "dernierRapportDirect": {"typePari": "SIMPLE_GAGNANT", "rapport": 3.5,
                                                                         "dateRapport": 1},
                 "dernierRapportReference": {"typePari": "SIMPLE_GAGNANT", "rapport": 5.0}},
                {"nom": "Beta", "numPmu": 2, "statut": "NON_PARTANT"},
            ]}
        if url.endswith("/C3"):
            return {"heureDepart": 1792929110000, "statut": "ARRIVEE_DEFINITIVE"}
        return {"programme": {"reunions": [{
            "numOfficiel": 1, "pays": {"code": "FRA"}, "hippodrome": {"libelleLong": "CHANTILLY"},
            "courses": [{"numOrdre": 3, "specialite": "PLAT", "heureDepart": 1792929110000},
                        {"numOrdre": 5, "specialite": "PLAT", "heureDepart": 1792939110000,
                         "statut": "ARRIVEE_DEFINITIVE"},
                        {"numOrdre": 4, "specialite": "TROT_ATTELE", "heureDepart": 1792929110000}]}]}}
    return fake


def test_live_odds_api(monkeypatch, tmp_path):
    c = _client(monkeypatch, tmp_path)
    from galoptrack import live
    calls = []
    monkeypatch.setattr(live, "_json", _fake_pmu(calls))
    assert c.get("/api/cotes/20261027/1/3").status_code == 401        # pas connecté
    c.post("/login", data={"password": "secret"})
    assert c.get("/api/cotes/2026-10-27/1/3").status_code == 400
    j = c.get("/api/cotes/20261027/1/3").get_json()
    assert calls[0].endswith("/27102026/R1/C3/participants")
    assert j["partants"][0] == {"num": 1, "nom": "ALPHA", "cote": 3.5, "cote_ref": 5.0, "heure_cote": 1,
                                "non_partant": False, "place": 2}
    assert j["arrivee_definitive"]
    assert j["partants"][1]["non_partant"] and j["partants"][1]["cote"] is None
    assert j["depart"] == 1792929110000 and j["historique"] == {}
    assert j["terminee"]                                              # départ passé : cotes figées
    n = len(calls)
    c.get("/api/cotes/20261027/1/3")                                   # servi par le cache
    assert len(calls) == n


def test_odds_snapshot(monkeypatch, tmp_path):
    from galoptrack import live
    from galoptrack.storage import LocalStorage
    monkeypatch.setattr(live, "_json", _fake_pmu([]))
    monkeypatch.setattr(live.time, "sleep", lambda s: None)
    st = LocalStorage(tmp_path)
    depart = 1792929110000
    # 4 h avant : relevé ; 10 min plus tard : trop tôt (relevé espacé) ; 1 h avant : relevé
    assert live.snapshot(st, now_ms=depart - 4 * 3600_000, day="20261025")["courses_relevees"] == 1
    assert live.snapshot(st, now_ms=depart - 4 * 3600_000 + 600_000, day="20261025")["espacees"] == 1
    assert live.snapshot(st, now_ms=depart - 3600_000, day="20261025")["courses_relevees"] == 1
    # 10 min après le départ : plus de relevé
    assert live.snapshot(st, now_ms=depart + 600_000, day="20261025")["courses_relevees"] == 0
    h = live.history(live.load_day(st, "20261025"), 1, 3)
    assert h == {"1": [[depart - 4 * 3600_000, 3.5], [depart - 3600_000, 3.5]]}


def test_race_over():
    from galoptrack import live
    assert live.race_over("ARRIVEE_DEFINITIVE", 10_000_000, 0)                 # courue avant l'heure prévue
    assert not live.race_over("PROGRAMMEE", 10_000_000, 10_000_000 + 60_000)  # départ retardé de 1 min
    assert live.race_over("PROGRAMMEE", 10_000_000, 10_000_000 + 6 * 60_000)
    assert not live.race_over(None, None, 10_000_000)


def test_odds_snapshot_route_needs_key(monkeypatch, tmp_path):
    monkeypatch.setenv("RELEVE_CLE", "abc")
    c = _client(monkeypatch, tmp_path)
    assert c.get("/api/releve-cotes").status_code == 403
    assert c.get("/api/releve-cotes?cle=faux").status_code == 403
