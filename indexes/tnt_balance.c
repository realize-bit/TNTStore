#include "tnt_centree.h"
#include <limits.h>  // for INT_MAX
#include <stdlib.h>
#include <stdio.h>

/***** 내부 정적 함수들 *****/

/* (1) 트리의 전체 노드 수를 센다. */
static int count_nodes(centree_node node) {
    if (node == NULL)
        return 0;
    return 1 + count_nodes(node->left) + count_nodes(node->right);
}

/* (2) in‐order 순회하면서 노드들을 배열에 저장한다.
       (배열에 담긴 순서는 BST의 키 순서와 일치함) */
static void collect_nodes(centree_node node, centree_node* arr, int* idx) {
    if (node == NULL)
        return;
    collect_nodes(node->left, arr, idx);
    arr[(*idx)++] = node;
    collect_nodes(node->right, arr, idx);
}

/* (3) 트리 전체를 순회하여, 자식이 없는 노드는 fixed(=1)로,
       내부 노드는 fixed(=0)로 표시한다. */
static void mark_fixed(centree_node node) {
    if (node == NULL)
        return;
    if (node->left == NULL && node->right == NULL) {
        node->removed = 1;  // fixed leaf
    } else {
        node->removed = 0;
        mark_fixed(node->left);
        mark_fixed(node->right);
    }
}

/* (4) in‐order 배열로부터 균형 잡힌 full binary tree를 재구성한다.
       인수 [l, r) 구간의 노드들은 원래 full 트리의 in‐order 순서로,
       전체 트리에서는 첫번째와 마지막 노드가 fixed (리프)여야 하며,
       내부(중간) 노드들은 fixed가 아니어야 한다.
       
       parent: 현재 서브트리의 부모 노드 (최상위 호출은 NULL)
       
       [리턴] 재구성된 서브트리의 루트 */
static centree_node build_tree_from_array(centree_node* nodes, int l, int r, centree_node parent) {
    int count = r - l;
    if (count == 0)
        return NULL;
    if (count == 1) {
        // 구간에 단 하나의 노드가 있으면(=리프) 그대로 리턴
        centree_node n = nodes[l];
        n->left = n->right = NULL;
        n->parent = parent;
        return n;
    }
    // 구간의 노드 수는 full binary tree이므로 홀수 (2m+1)이며, 
    // 구간의 첫, 마지막 노드는 fixed (리프)여야 한다.
    // 내부 노드 후보는 l+1, l+3, ..., r-2 (즉, 구간 내에서 홀수 인덱스)
    int best_k = -1;
    int best_diff = INT_MAX;
    for (int k = l+1; k < r-1; k += 2) {
        int left_size = k - l;
        int right_size = r - k - 1;
        int diff = left_size > right_size ? left_size - right_size : right_size - left_size;
        if (diff < best_diff) {
            best_diff = diff;
            best_k = k;
        }
    }
    if (best_k == -1) {
        fprintf(stderr, "Error in build_tree_from_array: no valid root found in segment [%d, %d).\n", l, r);
        exit(1);
    }
    // best_k에 해당하는 노드는 내부 노드여야 함.
    centree_node root = nodes[best_k];
    if (root->removed != 0) {
        fprintf(stderr, "Error: chosen root at index %d is fixed (originally leaf) but must be internal.\n", best_k);
        exit(1);
    }
    root->parent = parent;
    root->left = build_tree_from_array(nodes, l, best_k, root);
    root->right = build_tree_from_array(nodes, best_k+1, r, root);
    return root;
}

/***** centree_balance() 함수 *****/

/*
   centree_balance()
    - 밸런싱 시작 시점의 리프 노드(즉, 자식이 없는 노드)는 fixed 상태로 표시된다.
    - in‐order 순회 결과(전체 노드 수는 full tree이므로 홀수)는
      [fixed, internal, fixed, internal, …, fixed]의 순서를 갖는다.
    - build_tree_from_array()를 이용해 이 배열로부터 균형 잡힌 full binary tree를 재구성한다.
      이때, 내부 노드는 그대로 사용되고, fixed 노드는 반드시 자식 없이(leaf) 남게 된다.
    - 기존 노드들을 새롭게 연결하므로, 새 노드 할당이나 기존 노드 해제는 하지 않는다.
*/
void centree_balance(centree t) {
    if (t == NULL || t->root == NULL)
        return;
    
    /* (A) 원래 트리에서 자식이 없는 노드를 fixed(=1)로 표시 */
    mark_fixed(t->root);
    
    /* (B) 전체 노드 수를 센다.
           full binary tree이면 총 노드 수는 2n+1 (홀수)여야 한다. */
    int total_nodes = count_nodes(t->root);
    if (total_nodes == 0)
        return;
    if (total_nodes % 2 == 0) {
        fprintf(stderr, "Error: Tree does not have an odd number of nodes. (Not a full binary tree?)\n");
        return;
    }
    
    /* (C) in‐order 순회로 노드들을 배열에 저장한다. */
    centree_node* nodes = malloc(sizeof(centree_node) * total_nodes);
    if (nodes == NULL) {
        perror("malloc");
        exit(1);
    }
    int index = 0;
    collect_nodes(t->root, nodes, &index);
    if (index != total_nodes) {
        fprintf(stderr, "Error: Mismatch in node count: expected %d, got %d.\n", total_nodes, index);
        free(nodes);
        exit(1);
    }
    
    /* (D) 배열의 순서가 예상대로 fixed/내부 노드가 번갈아 나타나는지 확인 (전체 노드가 1개인 경우는 예외) */
    if (total_nodes > 1) {
        if (nodes[0]->removed != 1 || nodes[total_nodes-1]->removed != 1) {
            fprintf(stderr, "Error: In-order array does not start and end with fixed (leaf) nodes.\n");
            free(nodes);
            exit(1);
        }
        for (int i = 1; i < total_nodes-1; i++) {
            if (i % 2 == 1) { // 홀수 인덱스 → 내부 노드여야 함.
                if (nodes[i]->removed != 0) {
                    fprintf(stderr, "Error: Node at index %d is fixed but expected to be internal.\n", i);
                    free(nodes);
                    exit(1);
                }
            } else { // 짝수 인덱스 → fixed (리프)여야 함.
                if (nodes[i]->removed != 1) {
                    fprintf(stderr, "Error: Node at index %d is internal but expected to be fixed.\n", i);
                    free(nodes);
                    exit(1);
                }
            }
        }
    }
    
    /* (E) in‐order 배열을 이용해 균형 잡힌 full binary tree를 재구성한다.
           이 과정에서 모든 노드를 재사용하며, 새 할당은 하지 않는다. */
    centree_node new_root = build_tree_from_array(nodes, 0, total_nodes, NULL);
    new_root->parent = NULL;
    t->root = new_root;
    
    free(nodes);
}

