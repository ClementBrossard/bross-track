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
