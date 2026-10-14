#pragma once

/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <vector>
#include <string>
#include <unordered_map>
#include <atomic>
#include <memory>

#include "Job.hpp"


// DAG node structure
struct DAGNode {
    Job job;                              // Job information
    std::atomic<int> in_degree{0};        // In-degree
    std::vector<DAGNode*> successors;     // List of successor nodes
};

// JobDAG class for building and operating job dependency graph
class JobDAG {
public:
    // Constructor, takes job list and builds DAG
    explicit JobDAG(const std::vector<Job>& jobs);

    // Check if there is a cycle
    bool has_cycle() const;

    // Get initial nodes (nodes with in-degree 0)
    std::vector<DAGNode*> get_initial_nodes() const;

private:
    std::unordered_map<std::string, DAGNode*> key_to_node_; // Mapping from job key to node
    std::vector<std::unique_ptr<DAGNode>> nodes_;           // Storage for all nodes
};
