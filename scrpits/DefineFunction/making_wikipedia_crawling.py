import time
import random

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
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")

# ✅ 크롬 드라이버 실행
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 10)

url = "https://ko.wikipedia.org/wiki"
driver.get(url)


for idx, name in enumerate(ls):
    #  검색할 때마다 검색창을 다시 찾기
    search_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#searchInput")))

    #  검색어 입력 후 엔터 키 실행
    search_box.send_keys(name)
    search_box.send_keys(Keys.ENTER)

    #  페이지가 완전히 로딩될 때까지 대기
    wait.until(EC.presence_of_element_located((By.ID, "firstHeading")))

    #  HTML을 BeautifulSoup으로 파싱
    soup = BeautifulSoup(driver.page_source, "html.parser")
    time.sleep(random.uniform(2, 5))

    info_box = soup.select_one('#mw-content-text > div.mw-content-ltr.mw-parser-output > table')
    print(info_box)
    #  스크롤을 맨 위로 올리는 코드
    driver.execute_script("window.scrollTo(0, 0);")






"""
for name in ls:

    #2.  검색창이 나타날 때까지 기다림 (수정된 코드)
    search_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#searchInput')))

    # 클릭박스 찾기.
    click_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#searchform > button')))

    #3. 검색창 클릭
    search_box.click()

    #4. 검색어 입력
    search_box.send_keys(name)

    #5. Enter 키 입력
    driver.execute_script("arguments[0].click();", click_box)  # ✅ JavaScript로 클릭 실행


    time.sleep(1)

    #7. 제작비
    price = ''

    #8. 이동한 창의 HTML을 beautifulSoup 파싱
    soup = BeautifulSoup(driver.page_source, "html.parser")

    #9-1. 제작비가 박스에 존재하는 경우
    if soup.select_one('#mw-content-text > div:nth-of-type(1) > table:nth-of-type(2) > tbody > tr:nth-of-type(17) > td'):
        price = soup.select_one('#mw-content-text > div:nth-of-type(1) > table:nth-of-type(2) > tbody > tr:nth-of-type(17) > td').text

    #9-2. 제작비가 텍스트에 있는 경우
    elif soup.select_one('#mw-content-text > div.mw-content-ltr.mw-parser-output > p:nth-child(6)'):
        text = soup.select_one('#mw-content-text > div.mw-content-ltr.mw-parser-output > p:nth-child(6)').text
        real_cost = text.find('순제작비')
        total_cost = text.find('총제작비')

        # 9-2-1. 총 제작비가 존재할 경우
        if real_cost != -1:
            price = text[real_cost:real_cost+7]

        # 9-2-2. 총 제작비가 존재할 경우
        elif total_cost != -1:
            price = text[total_cost:total_cost+7]

        # 9-2-3. 둘 다 없을 경우
        else :
            price = '없음'

    else:
        price ='없음'

    print(name)



"""