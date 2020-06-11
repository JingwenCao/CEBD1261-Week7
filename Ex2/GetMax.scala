// Databricks notebook source
def max (x:Int, y:Int) : Int = {
  return x.max(y)
}

def get_max (x:Int, y:Int) : Int = {
  return max(x,y)
}
println(get_max(3, 5))
