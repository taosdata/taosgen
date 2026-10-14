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

#include "JobDAG.hpp"
#include <queue>

JobDAG::JobDAG(const std::vector<Job>& jobs) {
    // Build nodes
    for (const auto& job : jobs) {
        auto node = std::make_unique<DAGNode>();
        node->job = job;
        key_to_node_[job.key] = node.get();
        nodes_.push_back(std::move(node));
    }

    // Build edges
    for (const auto& job : jobs) {
        DAGNode* current = key_to_node_[job.key];
        for (const auto& dep_key : job.needs) {
            auto it = key_to_node_.find(dep_key);
            if (it == key_to_node_.end()) {
                throw std::runtime_error("Missing dependency: " + dep_key);
            }
            DAGNode* dep_node = it->second;
            dep_node->successors.push_back(current);
            current->in_degree++;
        }
    }
}

bool JobDAG::has_cycle() const {
    std::queue<DAGNode*> q;
    std::unordered_map<DAGNode*, int> in_degree_copy;
    size_t processed = 0;

    for (const auto& pair : key_to_node_) {
        in_degree_copy[pair.second] = pair.second->in_degree;
        if (in_degree_copy[pair.second] == 0) {
            q.push(pair.second);
        }
    }

    while (!q.empty()) {
        auto* node = q.front();
        q.pop();
        processed++;

        for (auto* successor : node->successors) {
            if (--in_degree_copy[successor] == 0) {
                q.push(successor);
            }
        }
    }

    return processed != key_to_node_.size();
}

std::vector<DAGNode*> JobDAG::get_initial_nodes() const {
    std::vector<DAGNode*> initial;
    for (const auto& pair : key_to_node_) {
        if (pair.second->in_degree == 0) {
            initial.push_back(pair.second);
        }
    }
    return initial;
}