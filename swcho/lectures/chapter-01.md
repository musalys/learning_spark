# 아파치 스파크 소개: 통합 분석 엔진 
***
### 단일화된 스택으로의 아파치 컴포넌트
+ <span style="background-color:gray">스파크SQL과 데이터프레임+데이터세트</span>
<span style="background-color:gray">스파크 정형화 스트리밍</span>
<span style="background-color:gray">스파크MLlib</span>
<span style="background-color:gray">GraphX</span>
<br/>
▶ <span style="background-color:gray">추출</span>
<span style="background-color:gray">변환</span>
<span style="background-color:gray">분석</span>
<span style="background-color:gray">변환</span>
작업 ❓
+ 자바, R, 스칼라, SQL, 파이썬 중 어느 것으로 스파크 코드를 작성하더라도 실제로는 바이트코드로 변환되어 클러스터 전체에 나뉘어 JVM에서 실행
<br/>
### 아파치 스파크의 분산 실행
+ 스파크 아키텍처: 
<span style="background-color:gray">스파크 애플리케이션(스파크 드라이버, 스파크 세션)</span>
<span style="background-color:gray">클러스터 매니저</span>
<span style="background-color:gray">스파크 이그제큐터</span>
+ 배포 모드: 
<span style="background-color:gray">로컬(local mode)</span>
<span style="background-color:gray">단독(standalone mode)</span>
<span style="background-color:gray">YARN(클라이언트)</span>
<span style="background-color:gray">YARN(클러스터)</span>
<span style="background-color:gray">쿠버네티스</span>
<br/>
▶ 로컬 모드와 단독 모드의 차이점: 로컬 모드에서는 모든 스파크 작업이 동일한 JVM 안에서 실행되므로 리소스 매니저 없이 실행되지만, 단독 모드는 여러 개의 JVM을 생성하므로 스파크의 자체 리소스 매니저가 JVM 처리를 맡는다.
### 스파크를 사용하는 이유
+ 데이터과학: 스파크 MLlib에서 지원하는 모듈을 사용하여 모델 파이프라인을 구축할 수 있는 일반적인 머신러닝 알고리즘 제공
+ 데이터 엔지니어링: 웹 애플리케이션이나 카프카 같은 스트리밍 엔진 등 다른 컴포넌트와 연계해 동작하는 분류 혹은 클러스터링 모델 구축 시, 더 큰 데이터파이프라인의 일부로 동작 시 사용(연산을 쉽게 병렬화)