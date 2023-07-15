# TileChunk

Chunk abstraction layer for convenient lam-layer implementation.

# Prerequisite
- Install "openblas" for  matrix multiplication.
``` 
sudo apt-get install libopenblas-dev
```

- Sparse BLAS support를 위한 GSL (GNU Scientific Library)를 설치합니다.
먼저, <https://ftp.jaist.ac.jp/pub/GNU/gsl/>에서 `gsl-2.7.tar.gz`를 다운 받습니다.
이후, tar file이 다운로드 된 디렉토리에서 아래를 수행합니다.
``` 
wget ftp.jaist.ac.jp/pub/GNU/gsl/gsl-2.7.tar.gz  
tar zxvf gsl-2.7.tar.gz
cd gsl-2.7.tar.gz
./configure && make && sudo make install
```
위의 instruction 대로 수행하면 각 헤더 파일들과 라이브러리 파일들이 default path (/usr/local/include, /usr/local/lib)에 각각 설치되므로, MakeFile에서 -I, -L 옵션을 통해 이들 path를 명시해주지 않아도 됩니다.
혹여 위의 default path가 아닌 다른 path에 GSL을 설치하였다면, compile 및 link 단계에서 해당 path들을 명시해줘야 합니다.

마지막으로 링크 단계에서 아래와 같이 -l 옵션을 추가합니다.
``` 
-lgsl -l{cblas library e.g. openblas, gslcblas etc}
``` 

보다 자세한 instruction은 <https://www.gnu.org/software/gsl/doc/html/usage.html>를 참고하시기 바랍니다.

# chunk_get_chunk_window()

## Parameters
- chunk_iter  :  현재 chunk iterator
- window_size :  window의 크기 (1차원 array)
- chunk       : 반환되는 chunk

## Parameter Details
### chunk_iter
하나의 chunk에 대한 window 연산을 수행할 때, 해당 chunk에 대해 `chunk_iterator`를 initialize하고 이를 `get_next()` 하면서 각 cell에 대한 window aggregate를 수행해야 합니다.  
현재 iteration 중인 `chunk_iterator`를 parameter로 넘겨주면, 그 cell의 위치를 center로 하는 적절한 크기의 window를 chunk의 형태로 반환해 줍니다.  
**현재 `get_chunk_window` 함수는 `custom_order_iterator`를 처리하지 못합니다. 반드시 `chunk_iterator_init()` 함수를 통해 iterator를 만들고 이 함수를 호출해주세요.**

### window_size
말 그대로 window의 크기입니다. 1차원 array 형태로, 예를 들어 `10*10`크기의 window를 원하면  
`window_size[0] = 10; window_size[1] = 10;` 이와 같이 설정하면 됩니다.

### chunk
반환되는 window chunk입니다.

## Example
이 함수를 사용하여 간단한 window 연산을 수행하는 primitive한 예시는 `chunktest.c`의 `test_get_chunk_window()`함수에 간단하게 묘사되어 있습니다.  
chunk의 각 cell마다 iteration 하면서 window를 생성하고, 이 window 안에서도 역시 iteration을 수행하며 aggregate를 수행하는 logic를 간단히 참고해주세요.