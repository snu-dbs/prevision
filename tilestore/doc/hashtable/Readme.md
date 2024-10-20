# HashTable Document

- File-Based HashTable (OpenHashing)
- Initial #Buckets  = 512 
    - 꽉찰 경우 자동으로 Bucket크기 2배씩 증가
- #Entries per bucket = 16 
- Duplicate Key의 경우 overwrite 
- Int64_t의 Key,Value 만 지원 
- 단순하게 구현하여 느릴 수 있음 

## Interface Example
```c
void test_hash_table_interface(){
    char* newht_name = "interface";

    // OPEN or CREATE hashtable
    StrDiskHT* hashtable = malloc(sizeof(StrDiskHT));
    tilestore_ht_open(newht_name, hashtable);

    int64_t  key = 1;
    int64_t  value = 3;

    // PUT key value
    tilestore_ht_put(hashtable, key, value);

    // GET key
    tilestore_ht_get(hashtable, key, &value);

    // PRINT ALL
    tilestore_ht_print_all(hashtable);

    // CLOSE
    tilestore_ht_close(hashtable);
    free(hashtable);

    printf("Hash Table Interace Test finished!!\n");
}

```

## Structure Description
<figure>
    <img src="/doc/img/hashtable.png" alt="Trulli" style="width:100%">
    <figcaption align="center"><b>Hashtable Description</b></figcaption>
</figure>