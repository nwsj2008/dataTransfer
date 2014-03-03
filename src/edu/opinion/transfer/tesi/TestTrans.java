package edu.opinion.transfer.tesi;

public class TestTrans {
	public static void main(String[] args) {
		TesiThread tesi = new TesiThread();
		Thread thread = new Thread(tesi);
		thread.start();
	}
}
