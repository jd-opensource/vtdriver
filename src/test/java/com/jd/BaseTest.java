/*
Copyright 2021 JD Project Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd;

public class BaseTest {

    protected void printNormal(String message) {
        System.out.println(message);
    }

    protected void printInfo(String message) {
        System.out.println("\033[1;34m" + message + "\033[0m");
    }

    protected void printComment(String message) {
        System.out.println("\033[1;30m" + message + "\033[0m");
    }

    protected void printOk(String message) {
        System.out.println("\033[1;32m" + message + "\033[0m");
    }

    protected String printFail(String message) {
        return "\033[1;31m" + message + "\033[0m";
    }
}
