# Scala-Spark
## 模式匹配遇到的方法
public static int digit(char ch,
                        int radix)

返回使用指定基数的字符 ch 的数值。
如果基数不在 MIN_RADIX <= radix <= MAX_RADIX 范围之内，或者 ch 的值是一个使用指定基数的无效数字，则返回 -1。如果以下条件中至少有一个为真，则字符是一个有效数字：

 
方法 isDigit 为 true，且字符（或分解的单字符）的 Unicode 十进制数值小于指定的基数。在这种情况下，返回十进制数值。
字符为 'A' 到 'Z' 范围内的大写拉丁字母之一，且它的代码小于 radix + 'A' - 10。在这种情况下，返回 ch - 'A' + 10。
字符为 'a' 到 'z' 范围内的小写拉丁字母之一，且它的代码小于 radix + 'a' - 10。在这种情况下，返回 ch - 'a' + 10。 （这里我一点也看不懂）
注：此方法无法处理增补字符。若要支持所有 Unicode 字符，包括增补字符，请使用 digit(int, int) 方法。

# Spark-mllib
## 重要python模块
### 学习numpy http://www.tuicool.com/articles/r2yyei 
### numpy.random 正则化 http://www.mamicode.com/info-detail-507676.html
