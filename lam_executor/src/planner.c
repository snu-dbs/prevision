//
// Created by mxmdb on 22. 1. 24..
//

#include <malloc.h>
#include <string.h>
#include <node_interface.h>
#include <assert.h>
#include "planner.h"
#include "exec_interface.h"

void expand_Range(Range range, uint32_t *dimlist, int listsize, uint64_t **dim_domains)
{
    for (int i = 0; i < listsize; i++)
    {
        range.lb[dimlist[i]] = dim_domains[dimlist[i]][0];
        range.ub[dimlist[i]] = dim_domains[dimlist[i]][1];
    }
}

void intersect_Range(Range range1, Range range2, Range dest)
{
    assert(range1.ndim == range2.ndim);
    /* dest = Range1  AND  Range2 */
    for (int d = 0; d < range1.ndim; d++)
    {
        dest.lb[d] = range2.lb[d] + range1.lb[d];
        dest.ub[d] = range2.ub[d] + range1.lb[d] <= range1.ub[d] ? range2.ub[d] + range1.lb[d] : range1.ub[d];
    }
}

Array *R_swap(Array *parent, Array *child)
{
    int op_type = child->op_type;

    if (op_type == OP_SCAN)
    {
        /** Example
         *          Subarray
         *              |
         *           Scan        =>          Subarray_Scan
         *             |                          |
         *             A                          A
         *
         */

        Array *new_parent = copy_node(parent);
        free(new_parent->arraylist);
        new_parent->arraylist = NULL;
        new_parent->array_num = 0;

        Array *child = parent->arraylist[0];
        char *arrayname = child->desc.object_name;
        new_parent->op_type = OP_SUBARRAYSCAN;
        char *sub = "_sub";
        new_parent->desc.object_name = malloc(strlen(arrayname) * sizeof(char) + 5);
        memcpy(new_parent->desc.object_name, arrayname, strlen(arrayname) * sizeof(char));
        memcpy(new_parent->desc.object_name + strlen(arrayname) * sizeof(char), sub, 5);
        free_node(child);
        free_node(parent);
        return new_parent;
    }
    else if (op_type == OP_TRANSPOSE)
    {

        /** Example
         *          Subarray                   Transpose
         *              |                         |
         *          Transpose        =>        Subarray
         *              |                         |
         *              A                         A
         *
         */

        Array *newchild = copy_node(parent);
        newchild->arraylist[0] = child->arraylist[0];

        Array *newparent = copy_node(child);
        newparent->arraylist[0] = newchild;

        uint32_t *order = newparent->op_param.trans.transpose_func;
        for (int d = 0; d < child->desc.dim_len; d++)
        {
            newchild->desc.dim_domains[order[d]][0] = parent->desc.dim_domains[d][0];
            newchild->desc.dim_domains[order[d]][1] = parent->desc.dim_domains[d][1];
            newchild->op_param.range.lb[order[d]] = parent->op_param.range.lb[d];
            newchild->op_param.range.ub[order[d]] = parent->op_param.range.ub[d];
            newparent->desc.dim_domains[d][0] = parent->desc.dim_domains[d][0];
            newparent->desc.dim_domains[d][1] = parent->desc.dim_domains[d][1];
        }

        free_node(child);
        free_node(parent);
        return newparent;
    }
    else if (op_type == OP_AGGR)
    {

        return copy_node(parent);
    }
    else if (op_type == OP_MATMUL)
    {

        /** Example
         *          Subarray                   MATMUL
         *              |                       /   \
         *           MATMUL        =>     Subarray  Subarray
         *            /  \                    /        \
         *           A     B                A           B
         *
         */

        /* SWAP */
        Array *leftNode = copy_node(parent);
        Array *rightNode = copy_node(parent);
        leftNode->arraylist[0] = child->arraylist[0];
        rightNode->arraylist[0] = child->arraylist[1];

        child->arraylist[0] = leftNode;
        child->arraylist[1] = rightNode;

        /* Expand Range */
        uint32_t E1R[1] = {1};
        uint32_t E0R[1] = {0};
        expand_Range(leftNode->op_param.range, E1R, 1, leftNode->arraylist[0]->desc.dim_domains);
        expand_Range(rightNode->op_param.range, E0R, 1, rightNode->arraylist[0]->desc.dim_domains);

        /* SET SUBARRAY NODE INFO */
        leftNode->desc.attr_type = leftNode->arraylist[0]->desc.attr_type;
        rightNode->desc.attr_type = rightNode->arraylist[0]->desc.attr_type;
        for (int d = 0; d < leftNode->desc.dim_len; d++)
        {
            leftNode->desc.dim_domains[d][0] = 0;
            leftNode->desc.dim_domains[d][1] = leftNode->op_param.range.ub[d] - leftNode->op_param.range.lb[d];
        }
        for (int d = 0; d < rightNode->desc.dim_len; d++)
        {
            rightNode->desc.dim_domains[d][0] = 0;
            rightNode->desc.dim_domains[d][1] = rightNode->op_param.range.ub[d] - rightNode->op_param.range.lb[d];
        }

        /* SET MATMUL NODE INFO */
        Array *newParant = copy_node(child);
        newParant->desc.attr_type = InferTypeBinaryOp(newParant->arraylist[0]->op_type, newParant->arraylist[1]->op_type);
        newParant->desc.dim_domains[0][0] = newParant->arraylist[0]->desc.dim_domains[0][0];
        newParant->desc.dim_domains[0][1] = newParant->arraylist[0]->desc.dim_domains[0][1];
        newParant->desc.dim_domains[1][0] = newParant->arraylist[1]->desc.dim_domains[1][0];
        newParant->desc.dim_domains[1][1] = newParant->arraylist[1]->desc.dim_domains[1][1];

        free_node(child);
        free_node(parent);
        return newParant;
    }
    else if (op_type == OP_ADD || op_type == OP_SUB || op_type == OP_DIV || op_type == OP_PRODUCT)
    {

        /** Example
         *          Subarray                     ADD
         *              |                       /   \
         *             ADD        =>      Subarray  Subarray
         *            /  \                    /        \
         *           A     B                A           B
         *
         */

        /* SWAP */
        Array *leftNode = copy_node(parent);
        Array *rightNode = copy_node(parent);
        leftNode->arraylist[0] = child->arraylist[0];
        rightNode->arraylist[0] = child->arraylist[1];

        child->arraylist[0] = leftNode;
        child->arraylist[1] = rightNode;

        for (int d = 0; d < leftNode->desc.dim_len; d++)
        {
            leftNode->desc.dim_domains[d][0] = 0;
            leftNode->desc.dim_domains[d][1] = leftNode->op_param.range.ub[d] - leftNode->op_param.range.lb[d];
        }

        for (int d = 0; d < 2; d++)
        {
            printf("DIM[%d]: %ld-%ld\n", d, parent->op_param.range.lb[d], parent->op_param.range.ub[d]);
        }

        /* SET SUBARRAY NODE INFO */
        leftNode->desc.attr_type = leftNode->arraylist[0]->desc.attr_type;
        rightNode->desc.attr_type = rightNode->arraylist[0]->desc.attr_type;
        for (int d = 0; d < leftNode->desc.dim_len; d++)
        {
            leftNode->desc.dim_domains[d][0] = 0;
            leftNode->desc.dim_domains[d][1] = leftNode->op_param.range.ub[d] - leftNode->op_param.range.lb[d];
        }
        for (int d = 0; d < rightNode->desc.dim_len; d++)
        {
            rightNode->desc.dim_domains[d][0] = 0;
            rightNode->desc.dim_domains[d][1] = rightNode->op_param.range.ub[d] - rightNode->op_param.range.lb[d];
        }

        /* SET MATMUL NODE INFO */
        Array *newParant = copy_node(child);
        newParant->desc.attr_type = InferTypeBinaryOp(newParant->arraylist[0]->op_type, newParant->arraylist[1]->op_type);
        for (int d = 0; d < newParant->arraylist[0]->desc.dim_len; d++)
        {
            newParant->desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
            memcpy(newParant->desc.dim_domains[d], newParant->arraylist[0]->desc.dim_domains[d], sizeof(uint64_t) * 2);
        }

        free_node(child);
        free_node(parent);
        return newParant;
    }
    else if (op_type == OP_SUBARRAY)
    {

        /** Example
         *          Subarray
         *              |
         *          Subarray        =>          Subarray
         *             |                          |
         *             A                          A
         *
         */

        Array *new_parant = copy_node(parent);
        Array *child = parent->arraylist[0];
        new_parant->arraylist[0] = child->arraylist[0];
        intersect_Range(child->op_param.range, parent->op_param.range, new_parant->op_param.range);
        free_node(child);
        free_node(parent);
        return new_parant;
    }
}

Array *push_down_R(Array *curNode)
{
    //    PrintArrayInfo(curNode);
    if (curNode->op_type == OP_SUBARRAY)
    {
        assert(curNode->array_num == 1);
        Array *curNodeNew = R_swap(curNode, curNode->arraylist[0]);
        return push_down_R(curNodeNew);
    }
    else
    {
        for (int i = 0; i < curNode->array_num; i++)
        {
            curNode->arraylist[i] = push_down_R(curNode->arraylist[i]);
        }
        return curNode;
    }
}

Array *merge_T(Array *curNode, Array *nextNode)
{

    /** Rule1 Merge T
     *
     *
     *          Transpose
     *              |
     *          Transpose        =>       Transpose
     *             |                          |
     *             A                          A
     */
    curNode->arraylist[0] = nextNode->arraylist[0];
    Array *copied = copy_node(curNode);
    for (int i = 0; i < curNode->op_param.trans.ndim; i++)
    {
        copied->op_param.trans.transpose_func[i] = nextNode->op_param.trans.transpose_func[curNode->op_param.trans.transpose_func[i]];
    }
    free_node(curNode);
    free_node(nextNode);
    return copied;
}

Array *swap_T(Array *parent, Array *child)
{
    int op_type = child->op_type;
    if (op_type == OP_MATMUL)
    {

        /**  MATMUL
         *
         *
         *                   Transpose [Parent]                           Binary Op [copiedChild]
         *                        |                                      /                 \
         *                Binary Op  [Child]       =>           Transpose [newLeft]  Transpose [newRight]
         *                   /             \                           |                      \
         *     Transpose [oldLeft]  (Transpose) [oldRight]          Transpose [oldRight]        (Transpose) [oldLeft]
         *                  |                |                       |                         \
         *                  A                B                      B                            A
         *
         */

        Array *copied_child = copy_node(child);
        Array *newLeft = copy_node(parent);
        Array *newRight = copy_node(parent);
        Array *oldLeft = child->arraylist[0];
        Array *oldRight = child->arraylist[1];

        uint32_t *order = parent->op_param.trans.transpose_func;

        for (int d = 0; d < child->desc.dim_len; d++)
        {
            newLeft->desc.dim_domains[d][0] = oldRight->desc.dim_domains[order[d]][0];
            newLeft->desc.dim_domains[d][1] = oldRight->desc.dim_domains[order[d]][1];
            newRight->desc.dim_domains[d][0] = oldLeft->desc.dim_domains[order[d]][0];
            newRight->desc.dim_domains[d][1] = oldLeft->desc.dim_domains[order[d]][1];
        }

        copied_child->arraylist[0] = newLeft;
        copied_child->arraylist[1] = newRight;

        newLeft->arraylist[0] = oldRight;
        newRight->arraylist[0] = oldLeft;

        for (int d = 0; d < copied_child->desc.dim_len; d++)
        {
            copied_child->desc.dim_domains[d][0] = parent->desc.dim_domains[d][0];
            copied_child->desc.dim_domains[d][1] = parent->desc.dim_domains[d][1];
        }
        free_node(parent);
        free_node(child);
        return copied_child;
    }
    else if (op_type == OP_ADD || op_type == OP_SUB || op_type == OP_DIV || op_type == OP_PRODUCT)
    {
        /**  ELEMWISE OP
         *
         *
         *                   Transpose [Parent]                           Binary Op [copiedChild]
         *                        |                                      /                 \
         *                Binary Op  [Child]       =>           Transpose [newLeft]  Transpose [newRight]
         *                   /             \                           |                      \
         *     Transpose [oldLeft]  (Transpose) [oldRight]          Transpose [oldLeft]        (Transpose) [oldRight]
         *                  |                |                       |                         \
         *                  A                B                      A                            B
         *
         */

        Array *copied_child = copy_node(child);
        Array *newLeft = copy_node(parent);
        Array *newRight = copy_node(parent);
        Array *oldLeft = child->arraylist[0];
        Array *oldRight = child->arraylist[1];

        uint32_t *order = parent->op_param.trans.transpose_func;

        for (int d = 0; d < child->desc.dim_len; d++)
        {
            newLeft->desc.dim_domains[d][0] = oldLeft->desc.dim_domains[order[d]][0];
            newLeft->desc.dim_domains[d][1] = oldLeft->desc.dim_domains[order[d]][1];
            newRight->desc.dim_domains[d][0] = oldRight->desc.dim_domains[order[d]][0];
            newRight->desc.dim_domains[d][1] = oldRight->desc.dim_domains[order[d]][1];
        }

        copied_child->arraylist[0] = newLeft;
        copied_child->arraylist[1] = newRight;

        newLeft->arraylist[0] = oldLeft;
        newRight->arraylist[0] = oldRight;

        for (int d = 0; d < copied_child->desc.dim_len; d++)
        {
            copied_child->desc.dim_domains[d][0] = parent->desc.dim_domains[d][0];
            copied_child->desc.dim_domains[d][1] = parent->desc.dim_domains[d][1];
        }
        free_node(parent);
        free_node(child);
        return copied_child;
    }
    else
    {
        fprintf(stderr, "NOT IMPLEMENTED \n");
    }
}

Array *reverse_swap_T(Array *curNode)
{
    int op_type = curNode->op_type;
    if (op_type == OP_MATMUL)
    {

        /** OP_MATMUL
         *
         *           Non Transpose                                       Non Transpose
         *                |                                                    |
         *               MatMul[curNode]                   =>              Transpose[newNode]
         *             /              \                                        |
         *     Transpose[oldLeft]  Transpose[oldRight]                      MatMul[curNode]
         *          |                 |                                     |      \
         *          A                 B                                    B         A
         *
         **/

        Array *oldLeft = curNode->arraylist[0];
        Array *oldRight = curNode->arraylist[1];

        Array *newNode = copy_node(oldLeft);
        Array *newCurNode = copy_node(curNode);

        uint32_t *order = curNode->op_param.trans.transpose_func;

        for (int d = 0; d < newNode->desc.dim_len; d++)
        {
            newNode->desc.dim_domains[d][0] = curNode->desc.dim_domains[d][0];
            newNode->desc.dim_domains[d][1] = curNode->desc.dim_domains[d][1];
        }
        newNode->arraylist[0] = newCurNode;

        Array *A = oldLeft->arraylist[0];
        Array *B = oldRight->arraylist[0];
        newCurNode->arraylist[0] = B;
        newCurNode->arraylist[1] = A;

        newCurNode->desc.dim_domains[0][0] = B->desc.dim_domains[0][0];
        newCurNode->desc.dim_domains[0][1] = B->desc.dim_domains[0][1];
        newCurNode->desc.dim_domains[1][0] = A->desc.dim_domains[1][0];
        newCurNode->desc.dim_domains[1][1] = A->desc.dim_domains[1][1];

        free_node(curNode);
        free_node(oldLeft);
        free_node(oldRight);
        return newNode;
    }
    else if (op_type == OP_ADD || op_type == OP_SUB || op_type == OP_DIV || op_type == OP_PRODUCT)
    {

        /** ELEMWISE OP
         *
         *           Non Transpose                                       Non Transpose
         *                |                                                    |
         *               ElemOP[curNode]                   =>              Transpose[newNode]
         *             /              \                                        |
         *     Transpose[oldLeft]  Transpose[oldRight]                      ElemOP[NewcurNode]
         *          |                 |                                     |      \
         *          A                 B                                    A         B
         *
         **/

        Array *oldLeft = curNode->arraylist[0];
        Array *oldRight = curNode->arraylist[1];

        Array *newNode = copy_node(oldLeft);
        Array *newCurNode = copy_node(curNode);

        for (int d = 0; d < newNode->desc.dim_len; d++)
        {
            newNode->desc.dim_domains[d][0] = curNode->desc.dim_domains[d][0];
            newNode->desc.dim_domains[d][1] = curNode->desc.dim_domains[d][1];
        }
        newNode->arraylist[0] = newCurNode;

        Array *A = oldLeft->arraylist[0];
        Array *B = oldRight->arraylist[0];
        newCurNode->arraylist[0] = A;
        newCurNode->arraylist[1] = B;

        for (int d = 0; d < curNode->desc.dim_len; d++)
        {
            newCurNode->desc.dim_domains[d][0] = A->desc.dim_domains[d][0];
            newCurNode->desc.dim_domains[d][1] = A->desc.dim_domains[d][1];
        }

        free_node(oldLeft);
        free_node(oldRight);
        free_node(curNode);
        return newNode;
    }
    else
    {
        fprintf(stderr, "NOT IMPLEMENTED \n");
    }
}

/** Heuristic Transpose folding
 *
 * Rule0 PushDown? PullUp?
 *
 *
 *
 *
 * Rule1 Merge T
 *
 *
 *          Transpose
 *              |
 *          Transpose        =>       Transpose
 *             |                          |
 *             A                          A
 *
 *
 * Rule2 Swap T
 *
 *
 *          Transpose                        Binary Op
 *              |                              /   \
 *          Binary Op         =>         Transpose  Transpose
 *             /  \                           |         \
 *     Transpose   (Transpose)          Transpose    (Transpose)
 *          |       |                       |           \
 *          A       B                      A(or B)      B(or A)
 *
 *
 *
 * Rule3 Reverse Swap T
 *
 *           Non Transpose                     Non Transpose
 *                |                                |
 * *          Binary Op         =>             Transpose
 *             /  \                                |
 *     Transpose   Transpose                    Binary Op
 *          |       |                          |       \
 *          A       B                      A(or B)      B(or A)
 *
 */

Array *fold_T(Array *curNode)
{
    //    PrintArrayInfo(curNode);
    if (curNode->op_type == OP_TRANSPOSE)
    {
        assert(curNode->array_num == 1);
        /* Identiy Check */
        bool identity = true;
        for (int i = 0; i < curNode->op_param.trans.ndim; i++)
        {
            if (curNode->op_param.trans.transpose_func[i] != i)
            {
                identity = false;
                break;
            }
        }
        if (identity)
        {
            Array *nextNode = curNode->arraylist[0];
            free_node(curNode);
            return fold_T(nextNode);
        }
        else
        {
            /* Rule1. Merge */
            Array *nextNode = curNode->arraylist[0];
            if (nextNode->op_type == OP_TRANSPOSE)
            {
                return fold_T(merge_T(curNode, nextNode));
            }
            else if (nextNode->array_num < 2)
            {
                for (int i = 0; i < curNode->array_num; i++)
                {
                    curNode->arraylist[i] = fold_T(curNode->arraylist[i]);
                }
                return curNode;
            }
            /* Rule2. Swap */
            else if (nextNode->array_num == 2)
            {
                bool left = false;
                bool right = false;
                if (nextNode->arraylist[0]->op_type == OP_TRANSPOSE)
                    left = true;
                if (nextNode->arraylist[1]->op_type == OP_TRANSPOSE)
                    right = true;

                if (!left && !right)
                {
                    for (int i = 0; i < curNode->array_num; i++)
                    {
                        curNode->arraylist[i] = fold_T(curNode->arraylist[i]);
                    }
                    return curNode;
                }
                else
                {
                    Array *newNode = swap_T(curNode, nextNode);
                    for (int i = 0; i < newNode->array_num; i++)
                    {
                        newNode->arraylist[i] = fold_T(newNode->arraylist[i]);
                    }
                    return newNode;
                }
            }
            fprintf(stderr, "ERROR\n");
        }
    }
    else
    {
        if (curNode->array_num == 2)
        {
            bool left = false;
            bool right = false;
            if (curNode->arraylist[0]->op_type == OP_TRANSPOSE)
                left = true;
            if (curNode->arraylist[1]->op_type == OP_TRANSPOSE)
                right = true;

            if (left && right)
            {
                bool same_order = true;
                Array *leftNode = curNode->arraylist[0];
                Array *rightNode = curNode->arraylist[1];
                for (int d = 0; d < curNode->arraylist[0]->op_param.trans.ndim; d++)
                {
                    if (leftNode->op_param.trans.transpose_func[d] != rightNode->op_param.trans.transpose_func[d])
                    {
                        same_order = false;
                        break;
                    }
                }
                if (same_order)
                {
                    /* Rule3. Reverse Swap */
                    Array *newNode = reverse_swap_T(curNode);
                    for (int i = 0; i < newNode->array_num; i++)
                    {
                        newNode->arraylist[i] = fold_T(newNode->arraylist[i]);
                    }
                    return newNode;
                }
            }
        }

        for (int i = 0; i < curNode->array_num; i++)
        {
            curNode->arraylist[i] = fold_T(curNode->arraylist[i]);
        }
        return curNode;
    }
}

void init_flag_DFS(Array *A)
{
    A->visited_flag = false;
    A->copied_array = NULL;

    for (int i = 0; i < A->array_num; i++)
    {
        init_flag_DFS(A->arraylist[i]);
    }
}

Array *copy_plan_sub(Array *src)
{
    Array *dest;

    if (src->visited_flag == false)
    {
        dest = copy_node(src);
        src->visited_flag = true;
        src->copied_array = dest;

        for (int i = 0; i < src->array_num; i++)
        {
            dest->arraylist[i] = copy_plan_sub(src->arraylist[i]);
        }
        return dest;
    }
    else
    {
        dest = src->copied_array;
        return dest;
    }
}

Array *copy_plan(Array *src)
{
    init_flag_DFS(src);
    return copy_plan_sub(src);
}

Array *expanded_copy_plan(Array *src)
{
    Array *dest;
    dest = copy_node(src);
    for (int i = 0; i < src->array_num; i++)
    {
        dest->arraylist[i] = expanded_copy_plan(src->arraylist[i]);
    }
    return dest;
}
Array *break_dag(Array *node)
{
    if (node->visited_flag == false)
    {
        node->visited_flag = true;
        for (int i = 0; i < node->array_num; i++)
        {
            node->arraylist[i] = break_dag(node->arraylist[i]);
        }
        return node;
    }
    else
    {
        return NULL;
    }
}

void merge_matmul_transpose(Array* in, uint8_t order) {
    if (in->visited_flag == order || in->executed) return;
    in->visited_flag++;

    // check current node is matmul
    if (in->op_type == OP_MATMUL) {
        // check any child is transpose
        Array *lhs = in->arraylist[0];
        Array *rhs = in->arraylist[1];

        // merge transpose child
        // merging mt for LHS is enabled only for a dense array 
        //   because it could require more computation if opnd is sparse array
        if (lhs->op_type == OP_TRANSPOSE && lhs->num_children == 1 && lhs->desc.array_type == TILESTORE_DENSE) {
            in->op_param.matmul.lhs_transposed = true;
            in->arraylist[0] = lhs->arraylist[0];
            // free_node(lhs);
        }

        if (rhs->op_type == OP_TRANSPOSE && rhs->num_children == 1) {
            in->op_param.matmul.rhs_transposed = true;
            in->arraylist[1] = rhs->arraylist[0];
            // free_node(rhs);
        }
    }

    // run dfs
    for (int i = 0; i < in->array_num; i++) {
        merge_matmul_transpose(in->arraylist[i], order);
    }
}

void free_plan_sub(Array *node)
{
    if (node == NULL)
        return;

    for (int i = 0; i < node->array_num; i++)
    {
        free_plan_sub(node->arraylist[i]);
    }
    free_node(node);
}

void free_plan(Array *node)
{
    init_flag_DFS(node);
    break_dag(node);
    free_plan_sub(node);
}

Array *gen_plan_R(Array *node)
{
    Array *plan1 = expanded_copy_plan(node);
    Array *plan2 = push_down_R(plan1);
    return plan2;
}

uint8_t gen_plan(Array *node)
{
    uint8_t order = 0;
    fprintf(stderr, "Input plan:\n");
    print_dag(node, ++order);

    // the below causes wrong result
    // Array* plan1 = expanded_copy_plan(node);
    // the belows have performance issue (maybe fold_T?)
    // Array *plan2 = push_down_R(node);
    // Array *plan3 = fold_T(plan2);

    merge_matmul_transpose(node, ++order);
    
    fprintf(stderr, "Optimized Plan:\n");
    print_dag(node, ++order);
    
    return order;
}
