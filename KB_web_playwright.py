from __future__ import annotations

import asyncio
import atexit
import datetime as dt
import os
import pickle
import re
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import git
import paramiko
from dotenv import load_dotenv
from playwright.sync_api import (
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    expect,
    sync_playwright,
)

import Focus_window
import client
import keys_qmenu
from logger import logger
from notify_telegram import simpleTelegram

logs = logger(Path(__file__).stem)

DEFAULT_MAIN_URL = "https://www.kbsec.com"
DEFAULT_LOGIN_URL = "https://www.kbsec.com/go.able"
DEFAULT_USER_AGENT = (
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/90.0.4430.229 Whale/2.10.123.42 Safari/537.36"
)

DEFAULT_WAIT_SECONDS = 15
DEFAULT_WAIT_MS = DEFAULT_WAIT_SECONDS * 1000
SHORT_SLEEP_SECONDS = 1
MANUAL_LOGIN_TIMEOUT_SECONDS = 300
HEARTBEAT_INTERVAL_SECONDS = 120
MAX_SERVICE_RETRIES = 6
MAX_LOGIN_LOOP_COUNT = 3
STOP_AFTER_TIME = dt.time(23, 45)

COOKIE_FILE = Path("cookie_KB.pickle")
OPENBANK_PICKLE_FILE = Path("KB.pickle")
WS_CONFIG_FILE = Path("ws.config")
DEBUG_FILE = Path("debug")
DEFAULT_USER_DATA_DIR = Path("Chrome")

LOGIN_FRAME_SELECTOR = 'iframe[name="LOGN010001-contentsFrame"]'
CERTIFICATE_FRAME_SELECTOR = 'iframe[name="yettie_sign_iframe"]'

load_dotenv()


def log_message(message: str, *, send: bool = False) -> None:
    logs.msg(message, send=send)


def normalize_user_agent(raw_user_agent: str) -> str:
    if raw_user_agent.startswith("user-agent="):
        return raw_user_agent.split("=", 1)[1]
    return raw_user_agent


def normalize_same_site(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    lowered = value.lower()
    if lowered == "lax":
        return "Lax"
    if lowered == "strict":
        return "Strict"
    if lowered in {"none", "no_restriction"}:
        return "None"
    return None


def normalize_cookie_for_playwright(cookie: dict[str, Any], fallback_url: str) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "name": cookie["name"],
        "value": cookie["value"],
    }
    domain = cookie.get("domain")
    path = cookie.get("path") or "/"
    if domain:
        normalized["domain"] = domain
        normalized["path"] = path
    else:
        normalized["url"] = cookie.get("url") or fallback_url
    if "secure" in cookie:
        normalized["secure"] = bool(cookie["secure"])
    if "httpOnly" in cookie:
        normalized["httpOnly"] = bool(cookie["httpOnly"])
    expires = cookie.get("expires", cookie.get("expiry"))
    if expires not in (None, "", 0):
        normalized["expires"] = float(expires)
    same_site = normalize_same_site(cookie.get("sameSite"))
    if same_site:
        normalized["sameSite"] = same_site
    return normalized


def save_pickle(path: Path, value: Any) -> None:
    with path.open("wb") as file_obj:
        pickle.dump(value, file_obj)


def load_pickle(path: Path) -> Any:
    with path.open("rb") as file_obj:
        return pickle.load(file_obj)


def run_async_compat(async_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    running_loop_getter = getattr(asyncio, "_get_running_loop", None)
    if callable(running_loop_getter):
        running_loop = running_loop_getter()
    else:
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

    if running_loop is None:
        return asyncio.run(async_fn(*args, **kwargs))

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            result["value"] = loop.run_until_complete(async_fn(*args, **kwargs))
            loop.run_until_complete(loop.shutdown_asyncgens())
        except BaseException as exc:
            error["value"] = exc
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()

    if "value" in error:
        raise error["value"]
    return result.get("value")


def string_to_int(value: str) -> int:
    digits = re.sub(r"[^\d-]", "", value or "")
    if digits in {"", "-"}:
        return 0
    return int(digits)


def read_ws_uri(default: str = "", *, announce: bool = False) -> str:
    if WS_CONFIG_FILE.is_file():
        ws_uri = WS_CONFIG_FILE.read_text(encoding="utf-8").strip()
        if announce:
            print(f"Using WS server: {ws_uri}")
        return ws_uri
    return default or os.getenv("WS_URI", "").strip()


def resolve_python_executable() -> str:
    venv_python = Path(".venv") / "Scripts" / "python.exe"
    if venv_python.is_file():
        return str(venv_python)
    return sys.executable


@dataclass
class KBConfig:
    ws_uri: str
    vm_addr: str
    vm_port: str
    vm_id: str
    vm_pswd: str
    kb_cpswd: str
    my_token: str
    users_id: str
    vm_qm_id: str
    chrome_user_data_dir: str
    headless: bool
    url_main: str = DEFAULT_MAIN_URL
    url_login: str = DEFAULT_LOGIN_URL
    user_agent: str = DEFAULT_USER_AGENT
    cookie_file: Path = COOKIE_FILE
    openbank_pickle_file: Path = OPENBANK_PICKLE_FILE

    @classmethod
    def load(cls) -> "KBConfig":
        load_dotenv()
        config = cls(
            ws_uri=read_ws_uri(os.getenv("WS_URI", ""), announce=True),
            vm_addr=os.getenv("VM_ADDR", ""),
            vm_port=os.getenv("VM_PORT", ""),
            vm_id=os.getenv("VM_ID", ""),
            vm_pswd=os.getenv("VM_PSWD", ""),
            kb_cpswd=os.getenv("KB_CPSWD", ""),
            my_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            users_id=os.getenv("TELEGRAM_USER_ID", ""),
            vm_qm_id=os.getenv("VM_QM_ID", "200"),
            chrome_user_data_dir=os.getenv(
                "CHROME_USER_DATA_DIR",
                str(DEFAULT_USER_DATA_DIR.resolve()),
            ),
            headless=not DEBUG_FILE.is_file(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        required_values = {
            "vm_addr": self.vm_addr,
            "vm_port": self.vm_port,
            "vm_id": self.vm_id,
            "vm_pswd": self.vm_pswd,
            "KB_cpswd": self.kb_cpswd,
            "my_token": self.my_token,
            "users_id": self.users_id,
            "uri": self.ws_uri,
        }
        missing = [key for key, value in required_values.items() if value in (None, "", [])]
        if missing:
            raise RuntimeError(f"Config 누락 값: {missing}")

    def refresh_ws_uri(self) -> None:
        self.ws_uri = read_ws_uri(self.ws_uri)


@dataclass
class RuntimeState:
    notifier: Optional[simpleTelegram] = None
    websocket: Any = None
    silent_shutdown: bool = False
    login_loop_count: int = 0


class LoginRetryLimitReached(RuntimeError):
    pass


@dataclass
class PlaywrightSession:
    user_data_dir: str
    headless: bool
    user_agent: str
    playwright: Any = None
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None
    browser_channel: Optional[str] = None

    def start(self) -> None:
        if self.playwright is None:
            self.playwright = sync_playwright().start()
        self.launch(self.headless)

    def launch(self, launch_headless: Optional[bool] = None) -> None:
        if self.playwright is None:
            self.playwright = sync_playwright().start()
        if launch_headless is not None:
            self.headless = launch_headless
        if self.context is not None:
            try:
                self.context.close()
            except Exception:
                pass
            self.context = None
            self.page = None

        common_kwargs = {
            "user_data_dir": self.user_data_dir,
            "headless": self.headless,
            "viewport": {"width": 1280, "height": 900},
            "locale": "ko-KR",
            "user_agent": normalize_user_agent(self.user_agent),
        }

        launch_errors: list[str] = []
        for browser_name, browser_channel in (("chromium", None), ("chrome", "chrome")):
            try:
                kwargs = dict(common_kwargs)
                if browser_channel is not None:
                    kwargs["channel"] = browser_channel
                self.context = self.playwright.chromium.launch_persistent_context(**kwargs)
                self.browser_channel = browser_name
                break
            except Exception as exc:
                launch_errors.append(f"{browser_name} :: {exc}")

        if self.context is None:
            raise RuntimeError(
                "Unable to launch Playwright browser: " + " | ".join(launch_errors)
            )

        self.context.set_default_timeout(DEFAULT_WAIT_MS)
        self.context.set_default_navigation_timeout(DEFAULT_WAIT_MS)
        self.context.on("page", self._register_page)
        self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
        self._register_page(self.page)

    def relaunch(self, launch_headless: bool) -> None:
        self.launch(launch_headless)

    def current_page(self) -> Page:
        if self.page is None:
            raise RuntimeError("Playwright page not initialized")
        return self.page

    def goto(self, url: str, wait_until: str = "domcontentloaded") -> Page:
        page = self.current_page()
        page.goto(url, wait_until=wait_until, timeout=DEFAULT_WAIT_MS)  # type: ignore
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeoutError:
            pass
        return page

    def save_cookies(self, cookie_path: Path, landing_url: str) -> bool:
        try:
            self.goto(landing_url)
            cookies = self.context.cookies() if self.context is not None else []
            save_pickle(cookie_path, cookies)
            return True
        except Exception as exc:
            log_message(f"Saving cookie failed :: {exc}")
            return False

    def add_cookies(self, cookies: list[dict[str, Any]], fallback_url: str) -> None:
        if self.context is None:
            raise RuntimeError("Browser context not initialized")
        playwright_cookies = [
            normalize_cookie_for_playwright(cookie, fallback_url)
            for cookie in cookies
            if cookie.get("name") and cookie.get("value") is not None
        ]
        if playwright_cookies:
            self.context.add_cookies(playwright_cookies)  # type: ignore

    def close(self) -> None:
        if self.context is not None:
            try:
                self.context.close()
            except Exception:
                pass
            self.context = None
        if self.playwright is not None:
            try:
                self.playwright.stop()
            except Exception:
                pass
            self.playwright = None
        self.page = None

    @staticmethod
    def _register_page(page: Page) -> None:
        page.on("dialog", lambda dialog: dialog.accept())


def setup_runtime(config: KBConfig) -> RuntimeState:
    runtime = RuntimeState(
        notifier=simpleTelegram(config.my_token, config.users_id),
    )
    logs.noty = runtime.notifier
    refresh_watchdog_connection(config, runtime)
    report_progress(runtime, __name__, 0)
    return runtime


def refresh_watchdog_connection(config: KBConfig, runtime: RuntimeState) -> None:
    config.refresh_ws_uri()
    runtime.websocket = client.watchgod_websocket(config.ws_uri)


def report_progress(runtime: RuntimeState, step_name: str, minutes: int) -> None:
    elapsed = 0
    while True:
        try:
            if runtime.websocket is not None:
                run_async_compat(runtime.websocket.send_msg, func_n=step_name)
        except Exception as exc:
            log_message(f"ws.send_msg failed :: {exc}")
        if elapsed >= minutes:
            return
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)
        elapsed += HEARTBEAT_INTERVAL_SECONDS // 60


def reset_login_loop_count(runtime: Optional[RuntimeState]) -> None:
    if runtime is None:
        return
    runtime.login_loop_count = 0


def increase_login_loop_count(runtime: RuntimeState, exc: Exception) -> None:
    runtime.login_loop_count += 1
    log_message(
        f"KB login failed ({runtime.login_loop_count}/{MAX_LOGIN_LOOP_COUNT}) :: {exc}"
    )
    if runtime.login_loop_count >= MAX_LOGIN_LOOP_COUNT:
        message = (
            f"KB login failed {runtime.login_loop_count} times in a row; stopping service"
        )
        log_message(message, send=True)
        raise LoginRetryLimitReached(message)


def should_continue_service(last_run: dt.datetime, runtime: RuntimeState) -> bool:
    while True:
        current = dt.datetime.now()
        if current.time() >= STOP_AFTER_TIME:
            return False
        next_allowed = (last_run + dt.timedelta(minutes=60)).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        if next_allowed <= current:
            return True
        report_progress(runtime, __name__, 5)


def login_frame(page: Page):
    return page.frame_locator(LOGIN_FRAME_SELECTOR)


def certificate_frame(page: Page):
    return login_frame(page).frame_locator(CERTIFICATE_FRAME_SELECTOR)


def is_logged_in(session: PlaywrightSession) -> bool:
    page = session.current_page()
    try:
        header = page.locator("#header")
        header.wait_for(state="visible", timeout=3000)
        return "로그아웃" in header.inner_text(timeout=3000)
    except Exception as exc:
        log_message(f"Login check failed: {exc}")
        return False


def wait_for_login(session: PlaywrightSession, timeout_seconds: int) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if is_logged_in(session):
            return True
        time.sleep(SHORT_SLEEP_SECONDS)
    return False


def submit_vm_certificate_password(config: KBConfig) -> None:
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(
        config.vm_addr,
        port=int(config.vm_port),
        username=config.vm_id,
        password=config.vm_pswd,
    )
    try:
        time.sleep(5)
        for key_value in config.kb_cpswd:
            cli.exec_command(
                f"sudo qm sendkey {config.vm_qm_id} {keys_qmenu.to_qmenu(key_value)}"
            )
            time.sleep(1)
        cli.exec_command(f"sudo qm sendkey {config.vm_qm_id} ret")
    finally:
        cli.close()


def perform_vm_login(config: KBConfig, page: Page) -> None:
    Focus_window.focus("KB증권")
    time.sleep(1)
    try:
        certificate_frame(page).locator("#passwordInput").click()
    except Exception:
        pass
    submit_vm_certificate_password(config)
    certificate_frame(page).locator("#passwordInput").press("Enter")


def perform_local_login(config: KBConfig, page: Page, session: PlaywrightSession) -> bool:
    password_input = certificate_frame(page).locator("#passwordInput")
    password_input.fill(config.kb_cpswd, timeout=10000)
    password_input.press("Enter")
    log_message("Wait until login done")
    return wait_for_login(session, MANUAL_LOGIN_TIMEOUT_SECONDS)


def execute_login(config: KBConfig, session: PlaywrightSession) -> bool:
    page = session.goto(config.url_login, wait_until="load")
    try:
        page.get_by_role("link", name="로그인").click()
        current_login_frame = login_frame(page)
        time.sleep(1)
        current_login_frame.get_by_label("자동로그아웃").select_option("28800000")
        time.sleep(1)
        current_login_frame.get_by_role("checkbox", name="PC방화벽").uncheck()
        time.sleep(1)
        current_login_frame.get_by_role("button", name="공동인증서로그인").click()

        if "TA-WIN" in socket.gethostname():
            perform_vm_login(config, page)
            return wait_for_login(session, DEFAULT_WAIT_SECONDS)
        return perform_local_login(config, page, session)
    except Exception as exc:
        log_message(f"Function KB_login ==> Failed :: {exc}")
        return False


def run_login_process(
    session: PlaywrightSession,
    config: KBConfig,
    runtime: Optional[RuntimeState] = None,
) -> bool:
    original_headless = session.headless
    try:
        log_message("KB login required", send=True)
        if original_headless:
            log_message("Switching to headed Playwright session for KB login")
            session.relaunch(False)

        if runtime is not None:
            report_progress(runtime, "KB_login_process", 0)

        if not execute_login(config, session):
            raise RuntimeError("KB login failed")

        session.save_cookies(config.cookie_file, config.url_main)
        reset_login_loop_count(runtime)
        return True
    except Exception as exc:
        log_message(f"Function KB_login_process => Failed :: {exc}")
        if runtime is not None:
            increase_login_loop_count(runtime, exc)
        return False
    finally:
        if original_headless and not session.headless:
            try:
                session.relaunch(True)
            except Exception as exc:
                log_message(f"Failed to relaunch headless Playwright session :: {exc}")


def load_or_create_cookies(
    session: PlaywrightSession,
    config: KBConfig,
    runtime: RuntimeState,
) -> list[dict[str, Any]]:
    try:
        cookies = load_pickle(config.cookie_file)
        if isinstance(cookies, list):
            return cookies
        raise TypeError("cookie file does not contain a list")
    except FileNotFoundError:
        log_message("No cookie found; continuing without saved cookies")
        return []
    except Exception as exc:
        log_message(f"Cookie load failed; continuing without saved cookies :: {exc}")
        return []


def restore_cookies(session: PlaywrightSession, config: KBConfig, cookies: list[dict[str, Any]]) -> None:
    page = session.goto(config.url_main)
    if not cookies:
        return
    session.add_cookies(cookies, config.url_main)
    page.reload(wait_until="domcontentloaded", timeout=DEFAULT_WAIT_MS)


def ensure_logged_in(session: PlaywrightSession, config: KBConfig, runtime: RuntimeState) -> None:
    session.goto(config.url_main)
    if is_logged_in(session):
        reset_login_loop_count(runtime)
        return
    if not run_login_process(session, config, runtime):
        raise RuntimeError("KB login failed")
    session.goto(config.url_main)
    if not is_logged_in(session):
        login_error = RuntimeError("KB login failed after relaunch")
        increase_login_loop_count(runtime, login_error)
        raise login_error
    reset_login_loop_count(runtime)


def open_openbank_page(session: PlaywrightSession) -> Page:
    page = session.current_page()
    page.get_by_role("link", name="뱅킹/대출", exact=True).hover()
    page.get_by_role("link", name="다른금융기관 계좌이체").click()
    page.get_by_role("link", name="계좌 한번에 불러오기 (어카운트인포)").click()
    return page


def confirm_optional_popup(page: Page, trigger: Callable[[], None]) -> None:
    try:
        with page.expect_popup() as popup_info:
            trigger()
        popup = popup_info.value
        popup.frame_locator("iframe").get_by_role("button", name="확인완료").click()
    except Exception:
        pass


def agree_openbank_terms(page: Page) -> None:
    page.locator('input[name="forAllAgree1"]').check()
    confirm_optional_popup(
        page,
        lambda: page.locator("span").filter(has_text="전체동의").nth(2).click(),
    )
    confirm_optional_popup(
        page,
        lambda: page.locator('input[name="forAllAgree5"]').check(),
    )
    page.get_by_role("button", name="다음").click()
    expect(page.locator('form[name="frm"]')).to_contain_text("다른금융사 계좌목록")


def scrape_openbank_accounts(page: Page) -> list[list[Any]]:
    page.get_by_role("table").wait_for(timeout=DEFAULT_WAIT_MS)
    rows = page.get_by_role("table").locator("tr")
    collected_rows: list[list[Any]] = []
    for index in range(rows.count()):
        row = rows.nth(index)
        cells = row.locator("td")
        if cells.count() < 5:
            continue
        bank_name = cells.nth(0).inner_text().strip()
        account_type = cells.nth(1).inner_text().strip()
        account_number = cells.nth(2).inner_text().strip()
        account_name = cells.nth(3).inner_text().strip()
        account_value = cells.nth(4).inner_text().strip()
        if not account_number or not account_value:
            continue
        collected_rows.append(
            [
                bank_name,
                account_type,
                account_number,
                account_name,
                string_to_int(account_value),
            ]
        )
    return collected_rows


def save_collection_results(
    session: PlaywrightSession,
    config: KBConfig,
    collected_rows: list[list[Any]],
) -> None:
    save_pickle(config.openbank_pickle_file, collected_rows)
    log_message(f"Saving results - OpenBank={len(collected_rows)}")
    if not session.save_cookies(config.cookie_file, config.url_main):
        raise RuntimeError("cookie save failed")


def run_stage(step_name: str, runtime: RuntimeState, action: Callable[[], Any]) -> Any:
    log_message(step_name)
    report_progress(runtime, step_name, 0)
    try:
        return action()
    except Exception as exc:
        log_message(f"Error:::{step_name}\n{exc}")
        raise


def run_collection_cycle(session: PlaywrightSession, config: KBConfig, runtime: RuntimeState) -> bool:
    try:
        cookies = run_stage(
            "2. Check cookie",
            runtime,
            lambda: load_or_create_cookies(session, config, runtime),
        )
        run_stage(
            "3. Load cookie",
            runtime,
            lambda: restore_cookies(session, config, cookies),
        )
        run_stage(
            "4. Login check",
            runtime,
            lambda: ensure_logged_in(session, config, runtime),
        )
        page = run_stage(
            "5. KB openbank",
            runtime,
            lambda: open_openbank_page(session),
        )
        run_stage(
            "5. KB openbank - AllChecks",
            runtime,
            lambda: agree_openbank_terms(page),
        )
        collected_rows = run_stage(
            "6. Data scraping",
            runtime,
            lambda: scrape_openbank_accounts(page),
        )
        run_stage(
            "7. Saving results",
            runtime,
            lambda: save_collection_results(session, config, collected_rows),
        )
        return True
    except LoginRetryLimitReached:
        raise
    except Exception:
        return False


def pull_latest_changes() -> None:
    try:
        print("Start update Process")
        repo = git.Repo("./")
        for item in repo.remotes.origin.pull():
            print(f"Updated {item.ref} to {item.commit}")
    except Exception as exc:
        print("Update Git Failed :", exc)


def run_kb_pipeline() -> None:
    completed = subprocess.run(
        [resolve_python_executable(), "KB.py"],
        check=False,
    )
    if completed.returncode != 0:
        log_message(f"KB.py exited with code {completed.returncode}")


def final_proc(session: PlaywrightSession, config: KBConfig, runtime: RuntimeState) -> None:
    try:
        session.save_cookies(config.cookie_file, config.url_main)
    except Exception as exc:
        log_message(f"final_proc save_cookies failed :: {exc}")
    try:
        session.close()
    except Exception as exc:
        log_message(f"final_proc session.close failed :: {exc}")
    if not runtime.silent_shutdown:
        log_message("KB 프로그램 종료", send=True)


def main() -> bool:
    try:
        config = KBConfig.load()
    except Exception as exc:
        log_message(f"KB 설정 로드 실패 :: {exc}")
        return False

    runtime = setup_runtime(config)
    log_message("KB 프로그램 시작", send=True)
    log_message("1. Prepare Playwright browser")
    if not config.headless:
        log_message("debug 파일 존재::: DEBUG 모드")

    session = PlaywrightSession(
        user_data_dir=config.chrome_user_data_dir,
        headless=config.headless,
        user_agent=config.user_agent,
    )
    session.start()
    atexit.register(final_proc, session, config, runtime)

    last_run = dt.datetime.now() - dt.timedelta(days=1)
    retries = 0
    while retries < MAX_SERVICE_RETRIES:
        retries += 1
        last_run = dt.datetime.now()
        refresh_watchdog_connection(config, runtime)

        try:
            cycle_completed = run_collection_cycle(session, config, runtime)
        except LoginRetryLimitReached:
            runtime.silent_shutdown = True
            return False

        if not cycle_completed:
            log_message("KB check 실패")
            continue

        session.save_cookies(config.cookie_file, config.url_main)
        pull_latest_changes()
        run_kb_pipeline()
        print("===Done===")

        retries = 0
        if not should_continue_service(last_run, runtime):
            runtime.silent_shutdown = True
            return False

    return False


if __name__ == "__main__":
    print("Last Modified: 260328 with Playwright")
    main()
