# lam_chunk_matmul()

## Parameters
- lhs     :  lhs chunk
- rhs     :  rhs chunk
- result  :  result chunk
- lhs_attrtype
- rhs_attrtype
- array_desc
- chunksize
- iteration  : for-loop의 iteration 횟수

## Parameter Details
### result chunk
lam_add 등과 달리, lam_matmul은 하나의 chunk를 완성하기 위해 여러번의 matmul이 호출되어야 합니다.
이 과정에서 하나의 result chunk에 matmul의 결과를 계속 더해 나가야 하기 때문에, 함수 밖에서 result chunk의 reference를 넘겨줘야 합니다.

pos에 맞는 result chunk를 함수 밖에서 `get_chunk()`한 이후, 그 chunk pointer를 `lam_chunk_matmul()`의 3번째 parameter로 넘겨주면 됩니다.
이에 따라 `lam_chunk_matmul()`는 pos를 parameter로 받지 않습니다.

### iteration
하나의 result chunk를 완성하기 위해 여러번의 matmul이 호출되는 과정에서, 첫번째 matmul의 결과는 result chunk에 overwrite하고, 그 이후의 matmul 결과는 여기에 더해나가야 합니다.
즉, 첫번째 matmul 호출이 edge case 입니다.
이 edge case handling을 함수 밖에서 하기는 어렵기 때문에 이를 함수 안에서 처리합니다. 
따라서 `lam_chunk_matmul()`을 호출할 때, 이것이 해당 result_chunk에 대한 첫번째 호출인지 아닌지를 구별해주는 parameter가 필요하고, 이것이 마지막 parameter인 `iteration`의 역할입니다.
첫번째 호출은 `0`으로 정의하며, 그 이외의 모든 integer는 첫번째 호출이 아닌것으로 간주합니다.

## Usage example
다음은 간단한 matmul: A * B = C 예제의 의사코드 입니다.
각 array의 스펙은 다음과 같습니다. :
- A : 100x1000-array, 100x100-chunk
- B : 1000x100-array, 100x100-chunk
- C : 100x100-array, 100x100-chunk

```c
chunk_get_chunk(name_C, chunk_size_C, chunk_coord_C, &chunk_C);

for (int i = 0; i < 10; i++)
{
    chunk_get_chunk(name_A, chunk_size_A, chunk_coord_A, &chunk_A);
    chunk_get_chunk(name_B, chunk_size_B, chunk_coord_B, &chunk_B);

    chunk_C = lam_chunk_matmul(chunk_A, chunk_B, chunk_C, TILESTORE_INT32, TILESTORE_INT32, array_desc, chunk_size, i);   // 0인 경우와 아닌 경우만 구분하면 됨.

    chunk_destroy(lhs_chunk);
    chunk_destroy(rhs_chunk);
}
```

## Trivial note
다른 연산들과 같이 `Chunk*`를 반환하도록 하였습니다.
같은 result chunk에 대하여 `lam_chunk_matmul()`을 반복적으로 호출하는 상황에서, 기존에 `get_chunk()`로 할당받은 chunk*를 넘겨줘도 되고, 이 반환된 chunk*를 넘겨줘도 됩니다. 
어차피 둘이 같은 포인터 입니다.
함수 안에서 세번째 parameter인 `result` chunk pointer를 그대로 반환합니다.


# lam_chunk_aggregate 
group by dim aggregate는 다음과 같이 총 2개의 함수로 나뉩니다.

# lam_chunk_group_by_dim_aggregate()
주어진 연산에 따라 aggregate를 수행하는 함수입니다.  
하나의 partition을 구성하는 각 chunk들에 대한 aggregation을 수행하고 그 결과를 `state`에 저장합니다.  

## Parameters
- opnd : Operand Chunk
- result : Result Chunk
- opnd_attr_Type : Operand array의 attribute data type (TILESTORE_INT32, TILESTORE_FLOAT32, TILESTORE_FLOAT64)
- state : 각 chunk에 대한 aggregation 결과를 저장하기 위한 intermediate memory space. Caller가 할당해서 넘겨줘야 합니다. 자세한 할당 방식은 아래에서 기술합니다. 
- func : 집계함수, `lam_interface.h`에 정의되어 있습니다.
- dim_selection_map : 각 dimension이 aggregation의 대상으로 선택되었는지 여부를 나타냅니다, `uint8_t`의 배열이고, 선택되었으면 1 아니면 0입니다. Caller가 만들어서 넘겨줘야 합니다. 자세한 할당방식은 아래에서 다룹니다.
- array_desc : array descriptor
- chunk_size : chunk_size
- iteration : for-loop의 iteration 횟수. `lam_chunk_matmul()`의 경우와 같습니다.

## Parameter Details
### result chunk
`lam_chunk_matmul()`의 경우와 같이, aggregation의 결과가 적힐 result chunk를 caller가 할당해서 넘겨줘야 합니다.    
하나의 result_chunk를 완성하기 위해 여러개의 opnd_chunk가 사용되기 때문입니다.  

pos에 맞는 result chunk를 함수 밖에서 `get_chunk()`한 이후, 그 chunk pointer를 `lam_chunk_group_by_dim_aggregate()`의 2번째 parameter로 넘겨주면 됩니다.    
이에 따라 `lam_chunk_group_by_dim_aggregate()`는 pos를 parameter로 받지 않습니다.    

### state
Partition을 구성하는 각 chunk에 대한 aggregation 중간 결과를 저장하기 위한 메모리 공간입니다.  
각 집계함수 별로 관리되는 정보는 다음과 같습니다.  
- count() : count
- sum() : sum
- avg() : count, sum
- max() : max
- min() : min
- var() : count, sum, square_sum
- stdev() : count, sum. square_sum

위와 같이, 관리되는 정보의 최대 개수는 3개입니다. 따라서 `lam_chunk_group_by_dim_aggregate()`의 호출 전에 `void** state = malloc(sizeof(double*) * 3);` 와 같이 state 메모리 공간을 할당해주고, 이를 4번째 parameter로 넘겨주면 됩니다.  

### dim_selection_map
`buffertile/util.c`에 `tiledb_util_get_selected_dim_by_names()`라는 함수가 있습니다. 이를 호출하여 dim_selection_map을 할당 받은 후, 이를 6번째 parameter로 넘겨주면 됩니다.

### iteration
State를 초기화 하기 위해 첫번쨰 iteration과 그 이후의 iteration을 구분해야 합니다.
`lam_chunk_matmul()`의 경우와 같이, 첫번째 호출은 `0`으로 정의하며, 그 이외의 모든 integer는 첫번째 호출이 아닌것으로 간주합니다.
for-loop의 iterator variable를 넣어주면 간편합니다.


# lam_group_by_dim_write()
Partition을 구성하는 모든 chunk에 대한 aggregate 수행 후, 그 중간 결과를 가공하여 최종결과를 write하는 함수입니다.

## Parameters
- result : result chunk
- state : aggregation의 중간 결과. 위에서와 같습니다.
- opnd_attr_type : Operand array의 attribute data type (TILESTORE_INT32, TILESTORE_FLOAT32, TILESTORE_FLOAT64)
- func : 집계함수, `lam_interface.h`에 정의되어 있습니다.

## Usage Example
아래는 다음 스펙의 array에 대한 group_by_dim_aggregate 예제의 의사코드 입니다.
아래의 예시와 같이, partition을 구성하는 모든 chunk에 대해 `lam_chunk_group_by_dim_aggregate()`를 호출 후, 마지막에 그 결과 `state`를 `lam_chunk_group_by_dim_write()` 함수에 넘겨주어 result array에 결과를 작성하면 됩니다.
- Operand Array A : 100*100*1000 array, 100*100*100 chunk
- Result Array B : 100*100 array, 100*100 chunk
- Group By Dim (d1, d2) - average()
```c
void **state;
uint8_t *dim_selection_map;

state = malloc(sizeof(double *) * 3);
tiledb_util_get_selected_dim_by_names(name_A, aggr_dim_name_list, dim_len, aggr_dim_len, &dim_selection_map);
chunk_get_chunk(name_B, chunk_size_B, chunk_coord_B, &chunk_B);

for (int i = 0; i < 10; i++) {
    chunk_get_chunk(name_A, chunk_size_A, chunk_coord_A, &chunk_A);
    lam_chunk_group_by_dim_aggregate(chunk_A, chunk_B, TILESTORE_INT32, state, AGGREGATE_AVG, dim_selection_map, array_desc, chunk_size, i);
    chunk_destroy(chunk_A);
}

lam_group_by_dim_write(chunk_B, state, TILESTORE_INT32, AGGREGATE_AVG);
chunk_destroy(chunk_B);
free(state);
tiledb_util_free_selected(&dim_selection_map);
```

