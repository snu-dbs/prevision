# BufferTile

[2021-1minirel](https://dbs.snu.ac.kr/git/chloe/2021-1minirel)의 BF layer 코드를
 기반으로 한 TileDB용 Buffer Manager.

본 `README.md`는 BufferTile을 빌드하는 방법과 API에 대해서 설명합니다.

## Table of Contents

[[_TOC_]]

## 빌드

### 준비사항

- TileDB 2.4.2
- gcc, makefile 등 빌드 도구들

### 프로젝트 빌드

1. [TileDB 2.4.2](https://github.com/TileDB-Inc/TileDB/archive/refs/tags/2.4.2.tar.gz) 버전을 빌드합니다.

```bash
wget https://github.com/TileDB-Inc/TileDB/archive/refs/tags/2.4.2.tar.gz
tar zxvf 2.4.2.tar.gz
cd TileDB-2.4.2
mkdir build; cd build
../bootstrap
make -j9
make -c install-tiledb
```

2. 이 repository를 clone 한 뒤, 빌드합니다.
  - `PATH_TO_TILEDB`는 위에서 TileDB를 빌드한 뒤 생성된 dist 디렉토리의 경로입니다.
    - 예시: `/media/samsung/mxm/TileDB-2.4.2/dist`

```bash
git clone ssh://git@dbs.snu.ac.kr:8999/mxmdb/buffertile.git
cd buffertile
make TILEDB_HOME="PATH_TO_TILEDB"
```

3. 생성된 `lib/libbf.a` 파일과 `include` 디렉토리를 적절히 사용합니다.

### 테스트

BufferTile을 테스트하기 위해서는, [프로젝트 빌드](#프로젝트-빌드)와 같이 빌드한 뒤, 아래 명령어를
  입력하시기 바랍니다.

```bash
PATH=$(pwd)/bin:"$PATH"; bftest
```

### 다른 프로젝트에서 사용

다른 프로젝트에서 buffertile을 사용하기 위해서는, 빌드 시 buffertile의 `include`와 위에와 생성된
 `libbf.a` 파일을 사용하면 됩니다. 이 때 TileDB의 path와 math library 또한 포함해야 합니다.
 아래 예시를 참고하세요.

```bash
PATH_TO_TILEDB=/home/grammaright/Workspace/mxm/TileDB-2.4.2/dist
PATH_TO_BUFFERTILE=/home/grammaright/Workspace/mxm/buffertile

export LD_LIBRARY_PATH="$PATH_TO_TILEDB:$LD_LIBRARY_PATH"

gcc main.c -L"$PATH_TO_TILEDB"/lib -L"$PATH_TO_BUFFERTILE"/lib -lbf -ltiledb -lm
 -I"$PATH_TO_TILEDB"/include -I"$PATH_TO_BUFFERTILE"/include
```

#### 예시 코드

아래는 domain을 {1, 128}로 가지고 tile extent는 16인 `hello_world`라는 배열에서, 17에서
 32까지의 좌표값을 가지는 cell들의 "a1" 값을 2배로 증가시키는 예시 코드입니다.

```c
#include <stdio.h>
#include "bf.h"

int main() {
  // variables
  PFpage *page;                                         // page pointer
  uint64_t dcoords[1] = {1};                            // tile coordinates
  struct array_key key = {"hello", "a1", dcoords, 1};   // array key

  BF_Init();                                            // Init BF Layer
  BF_Attach();                                          // Attach to the shm

  BF_GetBuf(key, &page);                                // Get page 

  for (int i = 0; i < 16; i++) {
    int value = tiledb_util_pagebuf_get_int(page, i);
    tiledb_util_pagebuf_set_int(page, i, value * 2);
  }

  BF_TouchBuf(key);                                     // Touch buffer
  BF_UnpinBuf(key);                                     // Unpin

  BF_Detach();                                          // Detach from the shm
  BF_Free();                                            // Free layer
  return 0;
}
```

## 메뉴얼

### API

BufferTile은 TileDB에 저장된 배열을 page(space tile) 단위로 가져올 수 있는 기능을 제공합니다.
 유저는 이 page를 읽거나 수정하여, TileDB에 저장되어 있는 배열을 조작할 수 있습니다. 모든 기능들은
 `bf.h`만 include하면 전부 사용할 수 있습니다.

제공하는 API는 다음과 같습니다. 

```c
int BF_Init();                              // BF Layer 초기화
void BF_Free();                             // BF Layer 정리
int BF_Attach();                            // Layer에 attach 및 tile ctx 생성
void BF_Detach();                           // Layer에서 detach 및 tile ctx free
int BF_GetBuf(array_key key, PFpage **page);// key에 대한 page 요청
int BF_UnpinBuf(array_key key);             // key에 대한 page unpin
int BF_TouchBuf(array_key key);             // key에 대한 page touch (dirty로 만듬)
```

`BF_Init`, `BF_Attach`, `BF_GetBuf` , `BF_UnpinBuf`, `BF_TouchBuf` 함수들의 
 return value는 error code를 나타냅니다. `0`(`BFE_OK`)이면 성공, 그 외에는 실패를 나타내고,
 자세한 내용은 `include/bf.h` 파일에 기록되어 있습니다.

함수들의 매개변수로 들어가는 `array_key`와 `page`는 아래에서 설명합니다.

#### array_key

BufferTile에서는 TileDB의 특정 tile을 나타내기 위해서 아래와 같은 `array_key`라는 자료구조를
 사용합니다. 위의 `BF_GetBuf` , `BF_UnpinBuf`, `BF_TouchBuf` 함수들은 전부 이를 이용하여
 page를 특정합니다.

```c
typedef struct array_key {
    char* arrayname;          // 배열의 이름(TileDB의 array-uri)
    char* attrname;           // attribute의 이름
    uint64_t *dcoords;        // tile의 coordinates
    uint32_t dim_len;         // tile coordinates의 길이
} array_key;
```

`dcoords` 변수는 tile의 coordinates를 나타내는 배열입니다. 이 좌표값은 배열이 어떤 domain을
 가지고 있던 간에 0에서부터 시작합니다. 예를들어, domain이 {1, 8, 3, 10}이고, tile extent가
 {3, 3}인 2치원 배열이 있다고 가정해 봅시다. dcoords={0, 0}은 {1, 3}에서부터 {3, 5}까지의
 cell들을 나타내게 됩니다. dcoords={2, 2}는 {7, 8}부터 {9, 10}까지입니다.

#### Page 

`BF_GetBuf` 함수를 이용하여 유저가 받는 page의 종류는 `PFpage` 단 하나입니다.
 배열의 종류, cell의 length, nullable에 따라 사용되는 멤버들이 다를 수 있는데, 이 때에는 type
 멤버를 사용하시면 됩니다.
 `type`은 `PFpage_type` 형이며, 아래와 같이 총 8가지로 구분됩니다.
 예를들어, DENSE 배열, fixed-length, nullable하지 않은 경우는 `DENSE_FIXED` 입니다.

유저는 반환받은 page는 `PFpage`는 shared memory 내에 존재합니다. 
 
Shared memroy는 각 process의 서로 다른 memory space로 매핑되기 때문에, pointer를
 사용할 수 없습니다. 따라서 BufferTile에서는 pointer로 관리해야 하는 데이터들을 pointer 타입이
 아닌 `BFshm_offset_t` 타입으로 관리합니다(일반 변수와 구분하기 위해서 변수명 끝에 `_o`이 붙여져
 있음). `PFpage`의 멤버 또한 마찬가지입니다. 이를 위의 layer에서 직접 접근하는 것은 추천하지
 않으며, 아래의 유틸리티 함수를 이용하는 것을 추천합니다. 이 값에 직접 접근하기 위해서는 
 `BF_SHM_PTR()` 메크로를 사용하시면 됩니다.

이 구조체들을 쉽게 사용하기 위해서 몇가지 유틸리티 함수들이 구현되어 있습니다(`include/utils.h`
 참고). 만약, 더 필요한 함수들이 있다면 직접 추가 바랍니다.

```c
typedef enum PFpage_type {
  DENSE_FIXED,              // dense and fixed length attribute
  DENSE_FIXED_NULLABLE,     // dense, fixed length, and nullable
  DENSE_VARIABLE,           // (deprecased) dense and variable length
  DENSE_VARIABLE_NULLABLE,  // (deprecased) dense, variable, and nullable 
  SPARSE_FIXED,             // sparse and fixed length attribute
  SPARSE_FIXED_NULLABLE,    // sparse, fixed length, and nullable
  SPARSE_VARIABLE,          // (deprecased) sparse, and variable length
  SPARSE_VARIABLE_NULLABLE  // (deprecased) sparse, variable length, and nullable
} PFpage_type;

typedef struct PFpage { 
  PFpage_type     type;                      // 타입      
  BFshm_offset_t  pagebuf_o;                 // (void*) 데이터
  uint64_t        pagebuf_len;               // pagebuf의 length (in bytes)
  uint64_t        unfilled_pagebuf_offset;   // pagebuf의 어디서부터 데이터를 쓸 수 있는지를 나타내는 offset 
  BFshm_offset_t  offset_o;                  // (uint64_t*) 각 cell들의 시작 offset(in bytes)을 저장한 배열.
  uint64_t        offset_len;                // offset의 length (in bytes)
  BFshm_offset_t  coords_o;                  // (void**) 각 cell들의 coordinates를 저장한 배열.
  BFshm_offset_t  coords_lens_o;             // (uint64_t*) coords의 길이
  uint64_t        unfilled_idx;              // offset, coords, validitiy의 몇번째 idx부터 쓸 수 있는지를 나타내는 값
  BFshm_offset_t  validity_o;                // (uint8_t*) Null bits
  uint64_t        validity_len;              // validity의 length (in bytes)
  uint32_t        dim_len;                   // num of dimensions
} PFpage; 
```

##### Dense vs. Sparse

![Dense vs. Sparse page](/doc/img/dense_and_sparse_pages.png)

PFpage는 특정 tile이 가지고 있는 데이터에 맞게 page size가 결정된다(가변 page size이기 때문). 예를들어 2x2 integer tile을 가진 dense array의 경우, PFpage의 pagebuf 크기는 4(# of cells) * 4(integer) = 16 bytes이다. 반면 sparse array의 경우, 단 2개의 cell만이 적혀있다고 했을 때 2(# of cells) * 4(integer) = 8 bytes이다. 즉, 위의 그림과 같이 같은 tile size를 가지고 있어도 (a) dense PFpage와 (b) sparse PFpage의 크기가 다를 수 있다.

Cell을 write 하고자 할 때, dense array의 경우에는 해당 cell에 대한 값만 수정하면 된다. 하지만 sparse array의 경우, 해당 cell에 대한 정보가 아직 page에 존재하지 않을 수 있다. 예를들어 2x2 ROW_MAJOR tile의 (1, 1) cell의 값을 쓰고자 한다고 가정했을 때, dense array의 경우 단순히 `pagebuf[3]`의 값만 수정하면 된다. 하지만 sparse array의 경우 (1, 1)이 `coords`에 존재하는지 확인한 뒤, 이에 따라 아래와 같은 방식으로 업데이트가 이루어져야 한다.
- 존재한다면, (1, 1)이 `coords`에서 몇번째 index인지 구한 뒤, 이 index에 해당하는 `pagebuf`, `offset`, `validity`를 수정해 주어야 한다.
- 존재하지 않는다면, page를 키운 뒤, `coords`, `pagebuf`, `offset`, `validity`를 추가해 주어야 한다.

###### Doubling the size of sparse page

Page size를 키우기 위해서는 page size를 두배로 키워주는  `tiledb_util_double_pagesize()` 함수를 사용하면 된다. 이렇게 page size를 키운 뒤에 어느 데이터까지 차있는지 알기 위해, `PFpage`는 `unfilled_pagebuf_offset`과 `unfilled_idx`라는 변수를 제공한다. `unfilled_pagebuf_offset`은 `pagebuf`에서 byte단위로 몇번째 offset부터 비어있는지를 나타내고, `unfilled_idx`는 offset, validity, coords가 몇번째 idx부터 비어있는지를 나타낸다. 이 둘이 구분되어 있는 이유는, TileDB에서 cell의 length는 1 이상일 수 있고, 이럴 경우 `pagebuf`의 길이를 `unfilled_idx`만으로 구할 수 없기 때문이다.

`unfilled_pagebuf_offset`과 `unfilled_idx` 이 두 변수는 유저가 조작하여야 한다. Cell을 write할 때 `unfilled_pagebuf_offset`와 `unfilled_idx`를 이용하여 데이터를 쓴 뒤, 이 값들을 증가시켜주어야 한다. 이를 편하게 하기 위해 util 함수에 아래와 같은 함수들을 추가해 두었다.

```C
uint64_t tiledb_util_pagebuf_get_unfilled_idx(PFpage *page);
void tiledb_util_pagebuf_set_unfilled_idx(PFpage *page, uint64_t new_unfilled_idx);
uint64_t tiledb_util_pagebuf_get_unfilled_pagebuf_offset(PFpage *page);
void tiledb_util_pagebuf_set_unfilled_pagebuf_offset(PFpage *page, uint64_t new_unfilled_pagebuf_offset);
```

![Doubling and filling data for sparse page](/doc/img/doubling_and_filling_data_for_sparse_page.png)

위 그림은 pagesize를 키운 뒤 데이터를 넣는 과정을 보여주는 예시이다. 먼저 (a)와 같이 데이터가 꽉 차있을 때 cell을 넣는다고 가정해보자. 이 때 유저는 `tiledb_util_double_pagesize()` 함수를 사용하여 page size를 2배로 늘려준다. (b)와 같이 page size가 2배로 늘어나도 `unfilled_pagebuf_offset`과 `unfilled_idx`는 유지된다. 유저는 이 값을 이용하여 적절한 `pagebuf`, `offset`, `validity`, `coords` 에 cell에 관한 정보를 작성하면 된다. 그 뒤에는 유저가 직접 `unfilled_pagebuf_offset`과 `unfilled_idx` 값을 위에서 언급한 함수들을 이용하여 조정해준다. 최종적으로 (c)와 같은 상태가 된다.

###### The default size of an empty sparse tile

Sparse tile의 경우 tile 안에 어떠한 데이터도 들어있지 않을 수 있다. 
이런 경우, BufferTile은 기본적으로 4개의 cell을 넣을 수 있을 만큼의 page size를 구성하여 유저에게 돌려준다.

Variable length attribute의 경우, variable attribute의 length가 8이라고 가정하여, 총 32 * sizeof(type) 만큼의 pagebuf를 만들어 준다.

##### Temp Page

BufferTile을 이용하여 in-memory에만 존재하는 temp page(혹은 temp tile)을 만들 수 있다.
BufferTile에 이러한 temp page를 요청하기 위해서는 `BF_GetTmpBuf()` 함수를 사용하면 된다.
`BF_GetTmpBuf()` 함수는 아래의 `temp_array_req` 구조체 변수를 인수로 받는다.

```C
typedef struct temp_array_req {
  PFpage_type         type;           // 이를 통해 nullable, sparse 여부를 파악함
  uint64_t            num_of_cells;   // Tile 내의 cell 개수
  tiledb_datatype_t   attr_type;      // Attribute의 타입 (single attribute 가정)
  uint32_t            dim_len;        // num of dimensions
} temp_array_req;
```

Temp page를 요청하기 위해서는 `temp_array_req`를 적절히 만들어 준 뒤 `BF_GetTmpBuf()`를
  호출하면 된다. 반환값은 두번째 인수인 `PFpage **page`를 통해서 받을 수 있으며, 기존의 
  `BF_GetBuf()`와 같은 형태로 반환된다.
Temp page를 unpin하기 위해서는 기존의 unpin 함수가 아닌, `BF_UnpinTmpBuf()` 함수를 
  사용하면 된다.

```C
// example
struct temp_array_req req = {DENSE_FIXED, 1024, TILEDB_INT32, 3};

// get temp page (tile)
PFpage *page;
BF_GetTmpBuf(req, &page);

// unpin the page. you cannot access the page right after the unpin.
BF_UnpinTmpBuf(page);
```  

##### Read Empty Tile 

TileDB array를 생성한 뒤, 아무것도 쓰지 않은 상태의 empty tile을 읽는 경우가 있다. 
예를들어 A array의 내용을 새로 생성한 B array로 그대로 복사하고자 할 때, 
  유저는 B array의 tile을 가져오기 위해 `BF_GetBuf()`를 통해 TileDB로부터 page
  를 읽어야 한다. 
이 때, 해당 tile은 아무것도 쓰여지지 않음에도 불구하고 I/O가 발생하고, 
  이로 인해 overhead가 발생한다.

이를 위해, BufferTile에서는 `BF_GetBufWithoutRead()` 함수를 제공한다.
함수의 인수는 `BF_GetBuf()`와 동일하다.
이 함수를 호출하게 되면, TileDB에서 데이터를 읽는 과정(I/O)는 생략한 채 page를 유저에게 반환한다.
그 외에 `BF_TouchBuf()`나 `BF_UnpinBuf()`는 동일하게 사용 가능하다.


## Known Issues

- [ ] Variable length dimension은 지원하지 않음.
- [ ] Cell 크기가 1 초과인 것들은 고려하지 않고 구현 중.