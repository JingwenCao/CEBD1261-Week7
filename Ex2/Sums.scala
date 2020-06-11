// Databricks notebook source
// MAGIC %scala
// MAGIC val nums = 1 to 45 toList
// MAGIC val divnums = nums.filter((x: Int) => x % 4 == 0)
// MAGIC val divby3 = nums.filter((x:Int) => x%3 == 0)
// MAGIC val divnums2 = divby3.filter((x:Int) => x<20)
// MAGIC 
// MAGIC def sum (a:List[Int]) : Int = {
// MAGIC   return a.reduce((x:Int, y:Int) => x + y)
// MAGIC }
// MAGIC println(sum(divnums))
// MAGIC println(sum(divnums2))
