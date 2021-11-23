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

package testsuite.util.printer;

public class TestSuitePrinter {
    private static final String NORMAL_NO_COLOR = "";

    private static final String INFO_BLUE_COLOR = "\033[1;34m";

    private static final String COMMENT_GREY_COLOR = "\033[1;30m";

    private static final String OK_GREEN_COLOR = "\033[1;32m";

    private static final String FAIL_RED_COLOR = "\033[1;31m";

    private static final String COLOR_SUFFIX = "\033[0m";

    private static final String DEFAULT_OK_MESSAGE = "[OK]";

    private static final String DEFAULT_FAIL_MESSAGE = "[FAIL]";

    protected static void printNormal(String message) {
        printNormal(message, Boolean.TRUE);
    }

    protected static void printNormal(String message, Boolean print) {
        print(NORMAL_NO_COLOR, message, print);
    }

    protected static void printInfo(String message) {
        printInfo(message, Boolean.TRUE);
    }

    protected static void printInfo(String message, Boolean print) {
        print(INFO_BLUE_COLOR, message, print);
    }

    protected static void printComment(String message) {
        printComment(message, Boolean.TRUE);
    }

    protected static void printComment(String message, Boolean print) {
        print(COMMENT_GREY_COLOR, message, print);
    }

    protected static void printOk() {
        printOk(DEFAULT_OK_MESSAGE, Boolean.TRUE);
    }

    protected static void printOk(Boolean print) {
        printOk(DEFAULT_OK_MESSAGE, print);
    }

    protected static void printOk(String message) {
        printOk(message, Boolean.TRUE);
    }

    protected static void printOk(String message, Boolean print) {
        print(OK_GREEN_COLOR, message, print);
    }

    protected static String printFail() {
        return FAIL_RED_COLOR + DEFAULT_FAIL_MESSAGE + COLOR_SUFFIX;
    }

    protected static String printFail(String message) {
        return FAIL_RED_COLOR + message + COLOR_SUFFIX;
    }

    private static void print(String prefix, String message, Boolean print) {
        if (!print) {
            return;
        }
        if (!prefix.equals("")) {
            message = prefix + message + COLOR_SUFFIX;
        }
        System.out.println(message);
    }
}
