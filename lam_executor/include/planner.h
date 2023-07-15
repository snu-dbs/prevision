//
// Created by mxmdb on 22. 1. 24..
//

#ifndef LAM_EXECUTOR_PLANNER_H
#define LAM_EXECUTOR_PLANNER_H

#endif //LAM_EXECUTOR_PLANNER_H

#include <array_struct.h>

void init_flag_DFS(Array* A);
Array* copy_plan_sub(Array* src);
Array* copy_plan(Array* src);
void free_plan(Array* node);


Array* push_down_R(Array* curNode);
Array* gen_plan_R(Array* node);
uint8_t gen_plan(Array *node);          // this return `order`, how many times the plan is traversed