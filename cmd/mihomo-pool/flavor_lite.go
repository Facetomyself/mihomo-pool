//go:build !embedded

package main

// buildFlavorConst 是构建形态标识。
//
// lite：调用外部 mihomo 二进制，体积小、可崩溃重启。
const buildFlavorConst = "lite"
