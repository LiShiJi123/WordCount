package com.LiShiJi.JAVA;


public class Test2 {
    int a = 10;
    static double b = 3.1;
    int c;
    Test2(int d){
        c=d;
    }
    void inGet(){
        System.out.println("a="+a);
        System.out.println("c="+c);
    }
    int setGet(int A){
        return A;
    }
    public static void main(String[] args) {
        Test2 p = new Test2(10);
        //System.out.println(p.a);
        p.inGet();
        System.out.println("b="+Test2.b);
        System.out.println("A="+p.setGet(5));
    }

}
