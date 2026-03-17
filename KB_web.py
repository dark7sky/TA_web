import chromedriver_autoinstaller
import pickle
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup as bs
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoAlertPresentException, NoSuchElementException, UnexpectedAlertPresentException
import sys
import time
import atexit
from logger import logger
import datetime
import os

import paramiko
import socket

import Focus_window
from dotenv import load_dotenv
import keys_qmenu

import client
import asyncio
import inspect
import functools
from typing import Callable, Any
from notify_telegram import simpleTelegram
import git
# import KB

# 타입 alias
WebDriver = webdriver.Chrome

# ./venv/Scripts/python -m PyInstaller -F KB_web.py --hidden-import=websockets.legacy --hidden-import=websockets.client --hidden-import=packaging --collect-data selenium --collect-data certifi
noty = None
headless = False
ws = None
logs = logger(os.path.basename(__file__).split(".")[0])
dbg = True
# 공통 대기/재시도 상수
DEFAULT_WAIT = 15
CLICK_RETRY = 5
SHORT_SLEEP = 1
MID_SLEEP = 2
load_dotenv()


def _env_list(key: str) -> list[str]:
    raw = os.getenv(key, "")
    if raw is None:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


REQUIRED_CONFIG_KEYS = [
    "creon_id",
    "creon_pswd",
    "creon_cpswd",
    "vm_addr",
    "vm_port",
    "vm_id",
    "vm_pswd",
    "KB_cpswd",
    "my_token",
    "users_id",
    "uri",
    "KBaccounts",
]
all_datas = {
    "url_main": "https://www.kbsec.com",
    "xpaths_login": [
        '//*[@id="loginAtag"]',
        '//*[@id="aos_chk1"]',
        '//*[@id="selTerm1"]',
        '//*[@id="selTerm1"]/option[6]',
        '//*[@id="tab-cont0"]/div/div/button',
        '//*[@id="passwordInput"]',
    ],
    "xpaths_login_frame": [
        "yettie_sign_iframe",
    ],
    "url_openbank": "https://www.kbsec.com/go.able?linkcd=s020110010000",
    "xpaths_view": [
        ['//*[@id="agreeTd"]/div[1]/span/span/label', "전체동의"],
        [
            '//*[@id="obAgreeDd6"]/span/label',
            "전체동의",
            '//*[@id="obAgreeDd9"]/div/span/label',
            "전체동의",
        ],
        "doMktCheck();",
        "#container > form > div.tbTy1 > table > tbody",
        "#assetWrite > tr:nth-child(1) > td:nth-child(3)",
        "#assetWrite > tr:nth-child(2) > td:nth-child(3)",
        "#assetWrite > tr:nth-child(3) > td:nth-child(3)",
        "#assetWrite > tr:nth-child(4) > td:nth-child(3)",
    ],
    "url_my": "https://www.kbsec.com/go.able?linkcd=m05010000",
    "my_token": os.getenv("TELEGRAM_BOT_TOKEN"),
    "users_id": os.getenv("TELEGRAM_USER_ID"),
    "url_creon": "https://www.creontrade.com",
    "url_creon_login": "https://www.creontrade.com/g.ds?m=2220&p=90&v=2422",
    "xpaths_creon": [
        '//*[@id="container"]/div[2]/div[1]/ul/li[2]/a',
        "#mainForm > div > div.myInfoWrap > div.myInfoArea > dl > dd:nth-child(2) > strong",
        'dassjs.cookie.setDsCookie("useKeyEnc","0")',
        '//*[@id="cmuc_id"]',
        '//*[@id="cmuc_pno"]',
        '//*[@id="pki_pno"]',
        ["timeout2", "5 시간"],
        "exec_signon()",
    ],
    "xpaths_KB_fint": [
        '//*[@id="assetWrite"]/tr[1]/td[5]/a[2]',
        [
            '//*[@id="grid_gridMain_data_td_',
            "_",
            '"]',
        ],  # 앞: 0~, 뒤: 1:종목명, 5:종목번호, 6:주문가능수량
        '//*[@id="trustedFund"]/strong',
    ],
    "url_creon_check": "https://www.creontrade.com/g.ds?m=4079&p=3241&v=3219",
    "useragent": "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.229 Whale/2.10.123.42 Safari/537.36",
    "Cookie_file": "cookie_KB.pickle",
    "Creon_logout": "javascript:goLogout();",
    "uri": os.getenv("WS_URI"),
    "KBaccounts": _env_list("KB_ACCOUNTS"),
    "vm_qm_id": os.getenv("VM_QM_ID", "200"),
}


def with_logging(logger: logger):
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger.msg(f"Function Start: {func.__name__}")
            value = func(*args, **kwargs)
            logger.msg(f"Function End: {func.__name__}")
            return value

        return wrapper

    return decorator


def dbg_msg(dbg: bool, msg: str, send: bool = False):
    if dbg:
        logs.msg(msg, send=send)


def save_cookies(driver: WebDriver, ckf: str) -> bool:
    dbg_msg(dbg, "0. Saving cookie")
    try:
        driver.get(all_datas["url_main"])
        a = driver.get_cookies()
        with open(ckf, "wb") as f:
            pickle.dump(a, f)
        return True
    except Exception as e:
        dbg_msg(dbg, f"0. Saving cookie ==> Failed :: {e}")
        return False


def wait_and_click(driver: WebDriver, xpth) -> bool:
    """표준화된 클릭 유틸: 다중 xpath 시 순차 시도, 명시적 대기 사용"""
    targets = xpth if isinstance(xpth, (list, tuple)) else [xpth]
    for target in targets:
        trial = 0
        while trial < CLICK_RETRY:
            trial += 1
            try:
                el = WebDriverWait(driver, DEFAULT_WAIT).until(
                    EC.element_to_be_clickable((By.XPATH, target))
                )
                el.click()
                return True
            except Exception as e:
                dbg_msg(dbg, f"{target} click failed (trial {trial}/{CLICK_RETRY}) :: {e}")
                time.sleep(SHORT_SLEEP)
                continue
    return False


def element_exists(driver: WebDriver, by: str, locator: str, wait: int = DEFAULT_WAIT) -> bool:
    """지정 요소 존재 여부를 명시적 대기로 확인"""
    try:
        WebDriverWait(driver, wait).until(EC.presence_of_element_located((by, locator)))
        return True
    except Exception as e:
        dbg_msg(dbg, f"element_exists fail :: {locator} :: {e}")
        return False


def health_check_basic(driver: webdriver.Chrome) -> bool:
    """KB/Creon 핵심 셀렉터 헬스체크 (로그인 전 접근 확인)"""
    ok = True
    try:
        driver.get(all_datas["url_main"])
        ok &= element_exists(driver, By.XPATH, all_datas["xpaths_login"][0])
    except Exception as e:
        dbg_msg(dbg, f"HealthCheck KB main failed :: {e}")
        ok = False

    try:
        driver.get(all_datas["url_creon_login"])
        ok &= element_exists(driver, By.XPATH, all_datas["xpaths_creon"][3])
        ok &= element_exists(driver, By.XPATH, all_datas["xpaths_creon"][4])
        ok &= element_exists(driver, By.XPATH, all_datas["xpaths_creon"][5])
    except Exception as e:
        dbg_msg(dbg, f"HealthCheck Creon failed :: {e}")
        ok = False
    return ok


@with_logging(logs)
def KB_login_check(driver: webdriver.Chrome) -> bool:
    """KB 로그인 상태 확인: 최대 대기 후 True/False 반환"""
    try:
        WebDriverWait(driver, DEFAULT_WAIT).until(
            lambda d: ("로그아웃" in d.page_source) or (d.find_elements(By.CLASS_NAME, "u-mn1"))
        )
    except Exception as e:
        dbg_msg(dbg, f"Login check wait failed :: {e}")
    # 로그인 여부 판정
    if driver.find_elements(By.CLASS_NAME, "u-mn1"):
        dbg_msg(dbg, "Not logged in")
        return False
    if "로그아웃" in driver.page_source:
        dbg_msg(dbg, "Logged in")
        return True
    dbg_msg(dbg, "Login indeterminate, treat as not logged in")
    return False


@with_logging(logs)
def KB_login(all_datas: dict, driver: webdriver.Chrome) -> bool:
    try:
        wait_and_click(driver, all_datas["xpaths_login"][0])
        WebDriverWait(driver, DEFAULT_WAIT).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "LOGN010001-contentsFrame"))
        )
        # wait_and_click(driver, all_datas["xpaths_login"][1])
        wait_and_click(driver, all_datas["xpaths_login"][2])
        wait_and_click(driver, all_datas["xpaths_login"][3])
        wait_and_click(driver, all_datas["xpaths_login"][4])
        aaaaa = inspect.currentframe()
        if aaaaa is None:
            aaaaa = "unknwon"
        else:
            aaaaa = aaaaa.f_code.co_name
        time_sleep_update_min(aaaaa, 0)
        time.sleep(MID_SLEEP)

        if "TA-WIN" in socket.gethostname():
            Focus_window.focus("KB증권")
            time.sleep(1)
            try:
                wait_and_click(driver, all_datas["xpaths_login"][5])
            except:
                pass
            cli = paramiko.SSHClient()
            cli.set_missing_host_key_policy(paramiko.AutoAddPolicy)
            cli.connect(
                all_datas["vm_addr"],
                port=int(all_datas["vm_port"]),
                username=all_datas["vm_id"],
                password=all_datas["vm_pswd"],
            )
            time.sleep(5)
            for xkey in all_datas["KB_cpswd"]:
                cli.exec_command(
                    "sudo qm sendkey "
                    + str(all_datas["vm_qm_id"])
                    + " "
                    + keys_qmenu.to_qmenu(xkey)
                )
                time.sleep(1)
            cli.exec_command("sudo qm sendkey " + str(all_datas["vm_qm_id"]) + " ret")
            cli.close()
        else:
            dbg_msg(dbg, "Wait until login done")
            while not KB_login_check(driver): time.sleep(SHORT_SLEEP)
        status = KB_login_check(driver)
    except Exception as e:
        dbg_msg(dbg, f"Funcion kb_login ==> Failed :: {e}")
        status = False
    return status


@with_logging(logs)
def KB_login_process(driver: webdriver.Chrome) -> bool:
    status = True
    try:
        dbg_msg(dbg, "KB Login required", True)
        if headless:
            #### headless option인 경우 기존 브라우저와 다른 새 브라우저를 띄워서 로그인할 때 ####
            options = webdriver.ChromeOptions()
            options.add_experimental_option("excludeSwitches", ["enable-logging"])
            options.add_argument(all_datas["useragent"])
            options.add_argument("lang=ko_KR")
            driver_temp = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver_temp.delete_all_cookies()
            driver_temp.set_window_position(0, 0)
            driver_temp.set_window_size(800, 800)
            driver_temp.implicitly_wait(5)
            driver_temp.get(all_datas["url_main"])
        else:
            #### 로그인에 기존 브라우저 이용 #####
            driver_temp = driver
        dbg_msg(dbg, "Login page entering")
        aaaaa = inspect.currentframe()
        if aaaaa is None:
            aaaaa = "unknwon"
        else:
            aaaaa = aaaaa.f_code.co_name
        time_sleep_update_min(aaaaa, 0)
        status &= KB_login(all_datas, driver_temp)
        if headless:
            driver_temp.quit()
        if not status:
            raise Exception(f"Login failed (status is {status})")
        dbg_msg(dbg, "Saving cookie and close browser")
        return status
    except:
        dbg_msg(dbg, "Function kb_login_process => Failed")
        return False


@with_logging(logs)
def check_and_click(driver: webdriver.Chrome, xpth: str, text: str) -> bool:
    global dbg
    trial = 0
    while trial < CLICK_RETRY:
        trial += 1
        try:
            btn = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.element_to_be_clickable((By.XPATH, xpth))
            )
            if text in btn.text:
                btn.click()
                return True
            dbg_msg(dbg, text + "(" + xpth + ") click failed. Re-try...")
        except Exception as e:
            dbg_msg(dbg, f"{text}({xpth}) click exception (trial {trial}/{CLICK_RETRY}) :: {e}")
        time.sleep(SHORT_SLEEP)
    return False


def check_coockie(filepath: str, driver: webdriver.Chrome) -> tuple[bool, Any]:
    trial = 0
    while trial <= 3:
        trial += 1
        try:
            with open(filepath, "rb") as f:
                cook = pickle.load(f)
            return True, cook
        except Exception as e:
            dbg_msg(dbg, f"No-cookie found or load failed (trial {trial}/3) :: {e}")
            if not KB_login_process(driver=driver):
                continue
    return False, None


def check_coockie_creon(filepath: str, driver: webdriver.Chrome):
    trial = 0
    while trial <= 3:
        trial += 1
        try:
            with open(filepath, "rb") as f:
                a = pickle.load(f)
            return a
        except Exception as e:
            dbg_msg(dbg, f"No-cookie found or load failed (Creon) trial {trial}/3 :: {e}")
            if not creon_login_process(driver=driver):
                continue
    sys.exit()


def close_all_popups(driver: webdriver.Chrome):
    try:
        while len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()
        driver.switch_to.window(driver.window_handles[0])
    except Exception as e:
        print(str(e))


def string_to_int(string: str) -> int:
    if "원" in string:
        string = string.replace("원", "")
    if "," in string:
        return int(string.replace(",", ""))
    else:
        return int(string)


def KB_data_convert(datain: str) -> list:
    while "\n\n\n" in datain:
        datain = datain.replace("\n\n\n", "\n\n")
    datain = datain.replace(" \n", "\n")
    datain = datain.replace("\n ", "\n")
    sel = datain.split("\n\n")
    while True:
        try:
            sel.remove("")
        except ValueError:
            break
    for n, s in enumerate(sel):
        sel[n] = s.rstrip().split("\n")  # type: ignore
        sel[n] = sel[n][0:5]
        sel[n][4] = string_to_int(sel[n][4])  # type: ignore
    return sel


def KB_fint(driver: webdriver.Chrome) -> bool:
    xdatas = all_datas["xpaths_KB_fint"]
    try:
        time.sleep(1)
        driver.get(all_datas["url_my"])
        time.sleep(1)
        driver.find_element(By.XPATH, xdatas[0]).click()
    except Exception as e:
        dbg_msg(dbg, "KB FINT계좌 상세 내역 조회 페이지 진입 실패" + "\n" + str(e))
        return False

    try:
        cash = driver.find_element(By.XPATH, xdatas[2]).text
        account = {"cash": string_to_int(cash), "stock": dict()}
    except Exception as e:
        dbg_msg(dbg, "KB FINT계좌 상세 내역 - 현금 조회 실패")
        return False

    ntmp = 0
    while True:
        try:
            name = driver.find_element(By.XPATH,xdatas[1][0] + str(ntmp) + xdatas[1][1] + "1" + xdatas[1][2],).text
            code = driver.find_element(By.XPATH,xdatas[1][0] + str(ntmp) + xdatas[1][1] + "5" + xdatas[1][2],).text
            balance = driver.find_element(By.XPATH,xdatas[1][0] + str(ntmp) + xdatas[1][1] + "6" + xdatas[1][2],).text
            account["stock"][code] = {"종목명": name, "수량": int(balance)}
            ntmp += 1
        except NoSuchElementException as e:
            if ntmp == 0:
                dbg_msg(dbg, f"KB FINT계좌 상세 내역 - 항목 없음 // {e}")
                return True
            else:   break

    with open("KB_fint.pickle", "wb") as f:
        pickle.dump(account, f)
    return True


def KB_routine(driver: webdriver.Chrome):
    """KB 오픈뱅크/마이페이지 스크랩 루틴 - 단계별 가드 및 예외 로그 강화"""
    status = True

    chapter = "2. Check coockie"
    time_sleep_update_min(chapter, 0)
    dbg_msg(dbg, chapter)
    try:
        status, cookies = check_coockie(all_datas["Cookie_file"], driver)
        if not status or cookies is None:
            raise Exception("cookie load failed")
    except Exception as e:
        dbg_msg(dbg, "Error:::" + chapter + "\n" + str(e))
        return False

    chapter = "3. Load coockie"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    try:
        driver.get(all_datas["url_main"])
        for cookie in cookies:
            driver.add_cookie(cookie)
        driver.refresh()
    except Exception as e:
        dbg_msg(dbg, "Error:::" + chapter + "\n" + str(e))
        return False

    chapter = "4. Login check"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    driver.get(all_datas["url_main"])
    try:
        status &= KB_login_check(driver)
        if not status:
            status = KB_login_process(driver)
        if not status:
            raise Exception("KB login failed")
    except Exception as e:
        dbg_msg(dbg, "Error:::4. Login check" + "\n" + str(e))
        return False

    chapter = "5. KB openbank"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    try:
        time.sleep(5)
        driver.get(all_datas["url_openbank"])
    except Exception as e:
        dbg_msg(dbg, "Error:::5. KB openbank" + "\n" + str(e))
        return False

    chapter = "5. KB openbank - AllChecks"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    try:
        status &= check_and_click(
            driver, all_datas["xpaths_view"][0][0], all_datas["xpaths_view"][0][1]
        )
        status &= check_and_click(
            driver, all_datas["xpaths_view"][1][0], all_datas["xpaths_view"][1][1]
        )
        status &= check_and_click(
            driver, all_datas["xpaths_view"][1][2], all_datas["xpaths_view"][1][3]
        )
        if not status:
            raise Exception("checkbox click failed")
    except Exception as e:
        dbg_msg(dbg, "Error:::5. KB openbank - AllChecks" + "\n" + str(e))
        return False

    chapter = "5. KB openbank - ClosePopups"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    try:
        close_all_popups(driver)
        dbg_msg(dbg, "5. KB openbank - Load datas")
        driver.execute_script(all_datas["xpaths_view"][2])
    except Exception as e:
        dbg_msg(dbg, "Error:::5. KB openbank - ClosePopups" + "\n" + str(e))
        return False

    chapter = "6. Data scrapping"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    try:
        html = driver.page_source
        html = bs(html, "html.parser")
        a = html.select_one(all_datas["xpaths_view"][3])
        if a == None:
            raise Exception('all_datas["xpaths_view"][3]')
        sel = KB_data_convert(a.text)
        driver.get(all_datas["url_my"])
        html = driver.page_source
        html = bs(html, "html.parser")
        kb_i = 0
        val_kb={}
        while True:
            kb_i += 1
            try:
                aa = html.select_one(f'#assetWrite > tr:nth-child({kb_i}) > td:nth-child(1)').text
                a = html.select_one(f'#assetWrite > tr:nth-child({kb_i}) > td:nth-child(3)').text
            except:
                print(f'Failed all_datas["xpaths_view"][{kb_i+3}]')
                break
            if aa == None or a == None: break
            for aaa in all_datas["KBaccounts"]:
                if aaa.startswith(aa.replace("-","").split("*")[0]):
                    val_kb.update({aaa: string_to_int(a)})
    except Exception as e:
        dbg_msg(dbg, "Error:::6. Data scrapping" + "\n" + str(e))
        return False

    chapter = "7. Saving results"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    try:
        with open("KB.pickle", "wb") as f: pickle.dump(sel, f)
        with open("KBself.pickle", "wb") as f: pickle.dump(val_kb, f)
        dbg_msg(
            dbg, "7. Saving results - OpnBk=" + str(len(sel)) + " / KB=" + str(val_kb)
        )
        status &= save_cookies(driver, all_datas["Cookie_file"])
    except Exception as e:
        dbg_msg(dbg, "Error:::7. Saving results" + "\n" + str(e))
        return False
    return status


def creon_login_check(driver: webdriver.Chrome) -> bool:
    status = True
    trial = 0
    while trial < CLICK_RETRY:
        trial += 1
        try:
            if driver.current_url == all_datas["url_creon_check"]:
                driver.get(all_datas["url_creon"])
            a = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_element_located((By.XPATH, all_datas["xpaths_creon"][0]))
            ).text
            if "로그인" in a:
                status = False
                break
            elif "로그아웃" in a:
                status = True
                break
        except (AttributeError, UnexpectedAlertPresentException) as e:
            dbg_msg(dbg, f"creon_login_check transient issue (trial {trial}/{CLICK_RETRY}) :: {e}")
            time.sleep(SHORT_SLEEP)
        except Exception as e:
            dbg_msg(dbg, f"creon_login_check error (trial {trial}/{CLICK_RETRY}) :: {e}")
            time.sleep(SHORT_SLEEP)
    return status


@with_logging(logs)
def creon_login(driver: webdriver.Chrome) -> bool:
    try:
        driver.get(all_datas["url_creon_login"])
        time.sleep(MID_SLEEP)
        try:
            alret = driver.switch_to.alert
            alret.accept()
            time.sleep(MID_SLEEP)
        except NoAlertPresentException:
            time.sleep(0.1)
        driver.execute_script(all_datas["xpaths_creon"][2])
        driver.refresh()
        time.sleep(SHORT_SLEEP)
        # 입력 단계에서 명시적 대기 추가
        WebDriverWait(driver, DEFAULT_WAIT).until(
            EC.presence_of_element_located((By.XPATH, all_datas["xpaths_creon"][3]))
        ).send_keys(all_datas["creon_id"])
        WebDriverWait(driver, DEFAULT_WAIT).until(
            EC.presence_of_element_located((By.XPATH, all_datas["xpaths_creon"][4]))
        ).send_keys(all_datas["creon_pswd"])
        WebDriverWait(driver, DEFAULT_WAIT).until(
            EC.presence_of_element_located((By.XPATH, all_datas["xpaths_creon"][5]))
        ).send_keys(all_datas["creon_cpswd"])
        Select(
            WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_element_located((By.ID, all_datas["xpaths_creon"][6][0]))
            )
        ).select_by_visible_text(all_datas["xpaths_creon"][6][1])
        time.sleep(SHORT_SLEEP)
        driver.execute_script(all_datas["xpaths_creon"][7])
        dbg_msg(dbg, "Wait until login done")
        status = creon_login_check(driver)
    except Exception as e:
        dbg_msg(dbg, f"Funcion creon_login ==> Failed :: {e}")
        status = False
    return status


@with_logging(logs)
def creon_login_process(driver: webdriver.Chrome) -> bool:
    status = True
    try:
        dbg_msg(dbg, "Creon Login required")
        time.sleep(MID_SLEEP)
        dbg_msg(dbg, "Login page entering")
        status &= creon_login(driver)

        return status
    except Exception as e:
        dbg_msg(dbg, f"Function creon_login_process => Failed :: {e}")
        return False


def creon_routine(driver: webdriver.Chrome) -> bool:
    status = True
    dbg_msg(dbg, "8. Login check")
    creon_trial = 0
    while creon_trial < 5:
        try:
            driver.get(all_datas["url_creon"])
            time.sleep(SHORT_SLEEP)
            driver.refresh()
            time.sleep(SHORT_SLEEP)
            status &= creon_login_check(driver)
            if not status:
                status = creon_login_process(driver)
            if not status:
                raise Exception("Creon login failed")
        except Exception as e:
            dbg_msg(dbg, "Error:::8. Login check" + "\n" + str(e))
            return False

        chapter = "9. Data scrapping"
        dbg_msg(dbg, chapter)
        time_sleep_update_min(chapter, 0)
        try:
            creon_trial += 1
            driver.get(all_datas["url_creon_check"])
            html = driver.page_source
            html = bs(html, "html.parser")
            a = html.select_one(all_datas["xpaths_creon"][1])
            if a == None:
                raise Exception("creon check element missing")
            a = a.text
            if a == "":
                dbg_msg(dbg, "Empty string")
                continue
            creon_val = string_to_int(a)
            driver.execute_script(all_datas["Creon_logout"])
            break
        except Exception as e:
            dbg_msg(dbg, "Error:::9. Data scrapping" + "\n" + str(e))
            return False
    else:
        dbg_msg(dbg, "Error:::08. Creon Check Failed")
        return False

    chapter = "10. Saving results"
    dbg_msg(dbg, chapter)
    time_sleep_update_min(chapter, 0)
    try:
        with open("CREON.pickle", "wb") as f:
            pickle.dump(creon_val, f)
        dbg_msg(dbg, "10. Saving results - CREON=" + str(creon_val))
    except Exception as e:
        dbg_msg(dbg, "Error:::10. Saving results" + "\n" + str(e))
        return False
    return status


def time_sleep_update_min(fn, minutes: int):
    """분 단위 대기: ws 실패 로그, 고정 120초 슬립"""
    counter = 0
    while True:
        try:
            asyncio.run(ws.send_msg(func_n=fn))
        except Exception as e:
            dbg_msg(dbg, f"ws.send_msg failed :: {e}")
        if counter >= minutes:
            break
        time.sleep(120)
        counter += 2


def timechecker(last_run: datetime.datetime):
    # 230409 HotTime이든 평소든 그냥 120분(2시간)에 한번씩 돌아가는 것으로 변경함.
    # opening = datetime.time(4, 59, 0, 0)
    # closing = datetime.time(23, 59, 0, 0)
    # hot_opening = datetime.time(8, 0, 0, 0)
    # hot_closing = datetime.time(17, 0, 0, 0)
    # dbg_msg(dbg, "Last run time: " + last_run.strftime("%m-%d %H:%M:%S"))
    while True:
        ttt = datetime.datetime.now()
        if (last_run + datetime.timedelta(minutes=60)).replace(minute=0, second=0, microsecond=0) <= ttt:
            return True
        time_sleep_update_min(__name__, 5)
        # if ttt.time() > closing or ttt.time() < opening:  # 클로징 타임 이후
        #     time_sleep_update_min(__name__, 0)
        #     return False
        # elif last_run.day != ttt.day:  # 날짜가 다른 경우 일단 실행
        #     return True
        # elif (hot_opening <= ttt.time() <= hot_closing) and ttt.weekday() < 5:  # 평일 핫타임
        #     if (last_run + datetime.timedelta(minutes=60)).hour <= ttt.hour:
        #         return True
        #     time_sleep_update_min(__name__, 5)
        # else:
        #     if (last_run + datetime.timedelta(minutes=120)).hour <= ttt.hour:
        #         return True
        #     time_sleep_update_min(__name__, 10)


def final_proc(driver: webdriver.Chrome, ckf: dict, silent: bool = False):
    save_cookies(driver, ckf["Cookie_file"])
    dbg_msg(dbg, "KB 프로그램 종료", not silent)


def main():
    global logs
    global noty
    global headless
    global ws
    ws_uri = os.getenv("WS_URI")
    if os.path.isfile("ws.config"):
        with open("ws.config", "r", encoding="utf-8") as f:
            ws_uri = f.read().strip()
            print(f"Using WS server: {ws_uri}")

    load_dotenv()
    confData = {
        "creon_id": os.getenv("CREON_ID"),
        "creon_pswd": os.getenv("CREON_PSWD"),
        "creon_cpswd": os.getenv("CREON_CPSWD"),
        "vm_addr": os.getenv("VM_ADDR"),
        "vm_port": os.getenv("VM_PORT"),
        "vm_id": os.getenv("VM_ID"),
        "vm_pswd": os.getenv("VM_PSWD"),
        "KB_cpswd": os.getenv("KB_CPSWD"),
        "my_token": os.getenv("TELEGRAM_BOT_TOKEN"),
        "users_id": os.getenv("TELEGRAM_USER_ID"),
        "uri": ws_uri,
        "KBaccounts": _env_list("KB_ACCOUNTS"),
        "vm_qm_id": os.getenv("VM_QM_ID", "200"),
    }
    all_datas.update(confData)  # type: ignore
    missing = [k for k in REQUIRED_CONFIG_KEYS if k not in all_datas or all_datas[k] in (None, "", [])]
    if missing:
        dbg_msg(dbg, f"Config 누락 키: {missing}", True)
        return False

    dbg_msg(False, "KB 프로그램 시작", True)
    dbg_msg(dbg, "1. Prepare chrome driver")
    noty = simpleTelegram(all_datas["my_token"], all_datas["users_id"])
    logs.noty = noty
    # logs = logger(os.path.basename(__file__).split(".")[0], noty)
    ws = client.watchgod_websocket(all_datas["uri"])
    time_sleep_update_min(__name__, 0)

    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_user_data_dir = os.getenv("CHROME_USER_DATA_DIR", os.path.abspath("Chrome"))
    options.add_argument(f"user-data-dir={chrome_user_data_dir}")
    if not os.path.isfile("debug"):
        options.add_argument("headless")
        headless = True
    else:
        dbg_msg(dbg, "debug 파일 존재::: DEBUG 모드")
        headless = False
    try: driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    # try: driver = webdriver.Chrome(r"115\chromedriver.exe", options=options)
    except:
        chromedriver_autoinstaller.install()
        driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(5)
    atexit.register(final_proc, driver, all_datas)
    status = True
    last_run = datetime.datetime.now() - datetime.timedelta(days=1)
    trial = 0
    while trial < 6:
        trial += 1
        last_run = datetime.datetime.now()
        if os.path.isfile("ws.config"):
            with open("ws.config", "r", encoding="utf-8") as f:
                all_datas["uri"] = f.read().strip()
            # print(f"Using WS server: {all_datas['uri']}")
        ws = client.watchgod_websocket(all_datas["uri"])
        status = True
        status &= KB_routine(driver)
        if not status:
            dbg_msg(dbg, "KB check 실패")
            continue
        status &= KB_fint(driver)
        if not status:
            dbg_msg(dbg, "KB Fint check 실패")
            continue
        # status &= creon_routine(driver)
        # if not status:
        #     dbg_msg(dbg, "Creon check 실패")
        #     continue
        save_cookies(driver, all_datas["Cookie_file"])
        # time_sleep_update_min(__name__, 0)
        # os.system(".\\dist\\KB.exe")
        # KB.KB_main()
        try:
            print("Start update Process")
            repo = git.Repo("./")
            origin = repo.remotes.origin
            info=origin.pull()
            for item in info:
                print(f"Updated {item.ref} to {item.commit}")
        except Exception as e: print("Update Git Failed : ",e)
        
        os.system(".\\venv\\Scripts\\python.exe KB.py")
        # time_sleep_update_min(__name__, 0)
        print("===Done===")
        trial = 0
        req = timechecker(last_run)
        if req == False:
            atexit.unregister(final_proc)
            atexit.register(final_proc, driver, all_datas, True)
            return False


if __name__ == "__main__":
    print(f"Last Modified: 260214 with Roo")
    main()
