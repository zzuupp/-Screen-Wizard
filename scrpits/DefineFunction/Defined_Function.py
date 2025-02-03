import pandas as pd
import requests
import time

class weekend_boxoffice:
    """
    주말 박스오피스에 관련된 모든 데이터를 수집하기 위한 클래스입니다.
    파라미터 값으로는 2개의 api값을 받아야합니다.
    Why?: 한 API당 일일 횟수 한도가 30000건인데,  
    
    """

    # 주말 박스오피스를 읽어오기 위한 기본 정보
    def __init__(self, api1, api2):
        self.api_1 = api1
        self.api_2 = api2


    def search_day(self, yyyymmdd):
        url = f'http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?key={self.api}&targetDt={yyyymmdd}'
        response = requests.get(url)
        return response.json()

    def concat_data(self, start, end):

        # 시작 날짜
        start_date = str(start)

        # 마지막 날짜
        end_date = str(end)

        # 합산될 데이터 : 빈 데이터로 초기화
        empty_df = pd.DataFrame()

        while int(start_date) < int(end_date):

            time.sleep(1)
            url = f'http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?key={self.api_1}&targetDt={int(start_date)}'
            response = requests.get(url)
            piece = response.json()
            # print(piece)
            # if 'weeklyBoxOfficeList' in piece['boxOfficeResult']:
            #     off_ls = piece['boxOfficeResult']['weeklyBoxOfficeList']
            # else:
            #     print(f"No weeklyBoxOfficeList for date: {start_date}")
            #     continue  # 또는 다른 적절한 처리를 수행

            piece_df = pd.DataFrame(piece['boxOfficeResult']['weeklyBoxOfficeList'])
            time.sleep(1)

            piece_df['showRange'] = piece['boxOfficeResult']['showRange']
            piece_df['yearWeekTime'] = piece['boxOfficeResult']['yearWeekTime']
            piece_df['boxofficeType'] = piece['boxOfficeResult']['boxofficeType']

            # 데이터 합치기.
            empty_df = pd.concat([empty_df, piece_df])


            #날짜 증가.
            start_date = pd.to_datetime(start_date) + pd.DateOffset(days=7)

            # 다시 문자열로 변환
            start_date = start_date.strftime('%Y%m%d')

        return empty_df


    def total_movie_info(self, movie_code):
        """
        영화코드는 1개 이상의 리스트 구조여야 합니다.
        일일 조회횟수가 최대 3000건이므로, 1500건 이상의 code가 들어온다면
        api_key를 분할하여 진행합니다.
        """


        movie_df = pd.DataFrame()
        genres_df = pd.DataFrame()
        directors_df = pd.DataFrame()
        actors_df = pd.DataFrame()
        showtypes_df = pd.DataFrame()
        companys_df = pd.DataFrame()



        for id, movicd in enumerate(movie_code):

            # api_key 변경 여부 정하기.
            if id <= 1500:
                url = f'http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={self.api_1}&movieCd={movicd}'

            else:
                url = f'http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={self.api_2}&movieCd={movicd}'

            response = requests.get(url)
            movie_info = response.json()

            #--- 데이터 추출.
            # 영화명
            movieNm = movie_info['movieInfoResult']['movieInfo']['movieNm']

            # 상영시간
            showTm = movie_info['movieInfoResult']['movieInfo']['showTm']

            # 제작연도
            prdtYear = movie_info['movieInfoResult']['movieInfo']['prdtYear']

            # 개봉연도
            openDt = movie_info['movieInfoResult']['movieInfo']['openDt']

            # 제작상태명
            prdtStatNm = movie_info['movieInfoResult']['movieInfo']['prdtStatNm']

            # 영화유형명
            typeNm = movie_info['movieInfoResult']['movieInfo']['typeNm']

            # 국가
            nations = movie_info['movieInfoResult']['movieInfo']['nations']

            if nations:  # nations 리스트가 비어 있지 않은 경우
                nation_name = nations[0]['nationNm']
            else:  # nations 리스트가 비어 있는 경우
                nation_name = None  # 또는 적절한 기본값 설정


            # 영화의 등급.
            #gradeNm = movie_info['movieInfoResult']['movieInfo']['audits'][0].get('watchGradeNm')
            audits = movie_info['movieInfoResult']['movieInfo'].get('audits', [])
            if audits:  # audits 리스트가 비어 있지 않은 경우
                gradeNm = audits[0].get('watchGradeNm')
            else:
                gradeNm = None  # 또는 적절한 기본값 설정

            # ------- 두개 이상의 값들이 들어있는 경우 : 테이블 따로 생성.
            genres = movie_info['movieInfoResult']['movieInfo']['genres']
            directors = movie_info['movieInfoResult']['movieInfo']['directors']
            actors =  movie_info['movieInfoResult']['movieInfo']['actors']
            showtypes = movie_info['movieInfoResult']['movieInfo']['showTypes']
            companys = movie_info['movieInfoResult']['movieInfo']['companys']


            # df 생성.
            extraction_dic = {'movieCd' : [movicd],
                            'movieNm' : [movieNm],
                            'showTm' : [showTm],
                            'prdtYear' : [prdtYear],
                            'openDt' : [openDt],
                            'prdtStatNm' : [prdtStatNm],
                            'typeNm' : [typeNm],
                            'nations':[nations],
                            'gradeNm':[gradeNm]}

            # movie
            insr_movie = pd.DataFrame(extraction_dic)

            # 장르


            insr_genres = pd.DataFrame(genres)
            insr_genres['movieCd'] = movicd

            insr_directors = pd.DataFrame(directors)
            insr_directors['movieCd'] = movicd

            insr_actors = pd.DataFrame(actors)
            insr_actors['movieCd'] = movicd

            insr_showtypes = pd.DataFrame(showtypes)
            insr_showtypes['movieCd'] = movicd

            insr_companys = pd.DataFrame(companys)
            insr_companys['movieCd'] = movicd

            time.sleep(1)

            # 데이터 합치기.
            # 데이터 합치기.
            movie_df = pd.concat([movie_df, insr_movie])
            genres_df = pd.concat([genres_df, insr_genres])
            directors_df = pd.concat([directors_df, insr_directors])
            actors_df = pd.concat([actors_df, insr_actors])
            showtypes_df = pd.concat([showtypes_df, insr_showtypes])
            companys_df = pd.concat([companys_df, insr_companys])

            time.sleep(1)

        return movie_df, genres_df, directors_df, actors_df, showtypes_df, companys_df


def combine_list_column(df, column):
    """
    특정 컬럼에 존재하는 리스트들을 합쳐주는 함수입니다.
    * parameter
    1. df : dataframe (리스트 컬럼이 존재하는 df)
    2. column : 리스트 컬럼 
    
    
    return 값은 수정된 df를 준다.
    """
    
    df[column] = df[column].apply(lambda x:x[0] if isinstance(x, list) and len(x) > 0 else x)
    return df


# 아직 패키지에 넣진 않았읍니다만, 어케 될지몰라 설명 달아놓슴다
def convert_to_hundred_million(value):
        """
        특정 컬럼에 존재하는 높은 숫자들을 보기 쉽게 변환하는 함수입니다.
        억 단위로 변환, 소수점 이하 2자리까지 표현됩니다.

        ex) 2.463506e+08 -> 2.463506×10^8  = 246,350,600
        2.46으로 표현되며 이 값은 2.46억입니다.
        소수점 이하 3번째 자리에서 반올림되며 이하 2자리까지 표현됩니다.
        """
        return round(value / 1e8, 2)