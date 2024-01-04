-- 배열 내의 중복제거
SELECT array_distinct(array(1, 2, 3, null, 3));

-- 중복되지 않은 두 배열의 교차점을 반환
SELECT array_intersect(array(1, 2, 3), array(1, 3, 5));

-- 중복 항목 없이 두 배열의 결합을 반환
SELECT array_union(array(1, 2, 3), array(1, 3, 5));

-- 배열1에는 존재하나 배열2에 존재하지 않는 요소를 중복 없이 반환
SELECT array_except(array(1, 2, 3), array(1, 3, 5));

-- 구분 기호를 사용하여 배열 요소를 연결
SELECT array_join(array('hello', 'world'), ' ');

-- Null 값은 건너뛰고 배열 내의 최댓값을 반환
SELECT array_max(array(1, 20, null, 3));

-- Null 값은 건너뛰고 배열 내의 최솟값을 반환
SELECT array_min(array(1, 20, null, 3));

-- Long 타입의 배열이 주어졌을 때 가장 첫 번째 인덱스의 요소를 반환
SELECT array_position(array(3, 2, 1), 1);

-- 지정된 배열에서 지정된 요소와 동일한 모든 요소를 제거
SELECT array_remove(array(1, 2, 3, null, 3), 3);

-- 배열1에 하나 이상의 null이 아닌 요소가 배열2에도 있는 경우 true를 반환
SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5));

-- 입력된 배열을 오름차순으로 정렬하고 Null 요소는 배열의 끝에 위치
SELECT array_sort(array('b', 'd', null, 'c', 'a'));

-- 문자열, 바이너리, 배열 등을 연결
SELECT concat(array(1, 2, 3), array(4, 5), array(6));

-- 배열 안의 배열들을 단일 배열로 플랫화
SELECT flatten(array(array(1, 2), array(3, 4)));

-- 지정된 요소가 포함된 배열을 지정한 횟수만큼 반환
SELECT array_repeat('123', 3);

-- 문자열의 역순 또는 배열에서 요소의 역순을 반환
SELECT reverse(array(2, 1, 4, 3));

-- 단계별로 시작부터 끝을 포함한 일련의 요소를 생성
SELECT sequence(1, 5);

-- 주어진 배열의 무작위 순열을 반환
SELECT shuffle(array(1, 20, null, 3));

-- 주어진 배열에서 지정된 길이의 지정된 인덱스에서 시작하는 하위 집합을 반환
SELECT slice(array(1, 2, 3, 4), -2, 2);

-- 병합된 구조 배열을 반환
SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4);)

-- 지정된 (1 기반) 인덱스에서 지정된 배열의 요소를 반환
SELECT element_at(array(1, 2, 3), 2);

-- 지정된 배열 또는 맵의 크기를 반환
SELECT cardinality(array('b', 'd', 'c', 'a'));

-- 주어진 키/값 배열 쌍에서 맵을 생성하여 반환. 키의 요소는 null을 허용하지 않음
SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'));

-- 주어진 배열에서 생성된 맵을 반환
SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));

-- 입력된 맵의 결합을 반환
-- 키가 중복일 때 에러 발생
-- Caused by: java.lang.RuntimeException: Duplicate map key 2 was found, please check the input data.
-- If you want to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to LAST_WIN so that the key inserted at last takes precedence.
SELECT map_concat(map(1, 'a', 2, 'b'), map(2, 'c', 3, 'd'));

-- 주어진 키에 대한 값을 반환하거나 키가 맵에 없는 경우 null을 반환
SELECT element_at(map(1, 'a', 2, 'b'), 2);

-- 지정된 배열 또는 맵의 크기를 반환
SELECT cardinality(map(1, 'a', 2, 'b'));