import time


# -----크롤링 관련
from bs4 import BeautifulSoup

# selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import StaleElementReferenceException
import undetected_chromedriver as uc






# ✅ 검색할 영화 리스트
ls = ['신과함께-죄와 벌', '스파이더맨: 노 웨이 홈', '스파이더맨: 파 프롬 홈']

# ✅ 크롬 옵션 설정 (브라우저 창 유지)
options = Options()
options.add_experimental_option("detach", True)

# ✅ 크롬 드라이버 실행
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 10)

# 🔹 검색 페이지로 이동 (매 검색마다 초기화 필요)
url = "https://ko.wikipedia.org/wiki"



for name in ls:
    movie_url = url+name
    driver.get(url)
    time.sleep(2)  # 페이지 로딩 대기
    # input_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#searchInput")))
    # input_box.clear()  # ✅ 기존 검색어 삭제
    # input_box.send_keys(name)
    # input_box.send_keys(Keys.ENTER)
   
    driver.implicitly_wait(time_to_wait= 10)
    # 원하는 요소가 로드 될 때까지 대기.
    wait.until(EC.presence_of_element_located((By.ID, "firstHeading")))

    # html 데이터 파싱
    soup = BeautifulSoup(driver.page_source, "html.parser")
    soup.prettify
    time.sleep(5)  # 페이지가 완전히 로드될 시간을 확보

