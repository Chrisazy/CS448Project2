/* ... */

package bufmgr;

import global.GlobalConst;
import global.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.Page;
import static global.SystemDefs.JavabaseDB;

public class BufMgr implements GlobalConst {

	private class FrameDescriptor {

		public PageId page_number = null;
		public int pin_count = 0;
		public boolean dirtybit = false;

	}

	private ArrayList<Integer>[] tab;
	private final int NBUF;
	private final int HTSIZE = 23;
	private final int SALT = 3;
	private final int PEPPER = 5;

	private String replacementPolicy;

	private byte[][] bufpool;
	private FrameDescriptor bufDescr[];

	private LinkedList<Integer> lruQueue;

	private int hash(int value) {
		return (SALT * value + PEPPER) % HTSIZE;
	}

	public void set(PageId pageNum, int fd) {
		int index = hash(pageNum.pid);
		tab[index].add(fd);
	}

	public void remove(PageId pageNum) {
		int index = hash(pageNum.pid);
		ArrayList<Integer> bucket = tab[index];
		for (int i = 0; i < bucket.size(); i++) {
			FrameDescriptor fd = bufDescr[bucket.get(i)];
			if (fd.page_number.pid == pageNum.pid) {
				bucket.remove(i);
				return;
			}
		}
	}

	public FrameDescriptor get(PageId pageNum) {
		int index = hash(pageNum.pid);
		ArrayList<Integer> bucket = tab[index];
		for (int i = 0; i < bucket.size(); i++) {
			FrameDescriptor fd = bufDescr[bucket.get(i)];
			if (fd.page_number.pid == pageNum.pid)
				return fd;
		}
		return null;
	}

	/**
	 * Create the BufMgr object. Allocate pages (frames) for the buffer pool in
	 * main memory and make the buffer manage aware that the replacement policy
	 * is specified by replacerArg (i.e. HL, Clock, LRU, MRU etc.).
	 *
	 * @param numbufs
	 *            number of buffers in the buffer pool.
	 * @param replacerArg
	 *            name of the buffer replacement policy.
	 */
	public BufMgr(int numbufs, String replacerArg) {
		this.NBUF = numbufs;
		bufpool = new byte[numbufs][];
		bufDescr = new FrameDescriptor[numbufs];
		lruQueue = new LinkedList<Integer>();
		for (int i = 0; i < numbufs; i++) {
			bufpool[i] = new byte[MINIBASE_PAGESIZE];
			bufDescr[i] = new FrameDescriptor();
			lruQueue.push(i);
		}
		replacementPolicy = replacerArg;
		tab = new ArrayList[HTSIZE];
		for (int i = 0; i < HTSIZE; i++) {
			tab[i] = new ArrayList<Integer>();
		}

	}

	/**
	 * Pin a page. First check if this page is already in the buffer pool. If it
	 * is, increment the pin_count and return a pointer to this page. If the
	 * pin_count was 0 before the call, the page was a replacement candidate,
	 * but is no longer a candidate. If the page is not in the pool, choose a
	 * frame (from the set of replacement candidates) to hold this page, read
	 * the page (using the appropriate method from {\em diskmgr} package) and
	 * pin it. Also, must write out the old page in chosen frame if it is dirty
	 * before reading new page. (You can assume that emptyPage==false for this
	 * assignment.)
	 *
	 * @param Page_Id_in_a_DB
	 *            page number in the minibase.
	 * @param page
	 *            the pointer poit to the page.
	 * @param emptyPage
	 *            true (empty page); false (non-empty page)
	 * @throws IOException 
	 * @throws FileIOException 
	 * @throws InvalidPageNumberException 
	 */
	public void pinPage(PageId pin_pgid, Page page, boolean emptyPage) throws InvalidPageNumberException, FileIOException, IOException {
		FrameDescriptor fd = get(pin_pgid);
		if (fd != null) {
			fd.pin_count++;
		} else {
			int fdid = getReplacement();
			fd = bufDescr[fdid];
			if (fd.page_number != null)
				remove(fd.page_number);
			fd.page_number = pin_pgid;

			JavabaseDB.read_page(pin_pgid, page);
			for (int i = 0; i < page.getpage().length; i++) {
				bufpool[fdid][i] = page.getpage()[i];
			}
			fd.pin_count++;
			fd.page_number = pin_pgid;
		}
	}

	private int getReplacement() {
		while (!lruQueue.isEmpty()) {
			int fdid = lruQueue.pop();
			FrameDescriptor fd = bufDescr[fdid];
			if (fd.pin_count == 0)
				return fdid;
		}
		return -1;
	}

	/**
	 * Unpin a page specified by a pageId. This method should be called with
	 * dirty==true if the client has modified the page. If so, this call should
	 * set the dirty bit for this frame. Further, if pin_count&gt;0, this method
	 * should decrement it. If pin_count=0 before this call, throw an exception
	 * to report error. (For testing purposes, we ask you to throw an exception
	 * named PageUnpinnedException in case of error.)
	 *
	 * @param globalPageId_in_a_DB
	 *            page number in the minibase.
	 * @param dirty
	 *            the dirty bit of the frame
	 */
	public void unpinPage(PageId pageNum, boolean dirty) {
		
	};

	/**
	 * Allocate new pages. Call DB object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page and pin it. (This call
	 * allows a client of the Buffer Manager to allocate pages on disk.) If
	 * buffer is full, i.e., you can't find a frame for the first page, ask DB
	 * to deallocate all these pages, and return null.
	 *
	 * @param firstpage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 *
	 * @return the first page id of the new pages. null, if error.
	 */
	public PageId newPage(Page firstpage, int howmany) {
	};

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 *
	 * @param globalPageId
	 *            the page number in the data base.
	 */
	public void freePage(PageId globalPageId) {
	};

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 *
	 * @param pageid
	 *            the page number in the database.
	 */
	public void flushPage(PageId pageid) {
	};

	/**
	 * Flushes all pages of the buffer pool to disk
	 */
	public void flushAllPages() {
	};

	/**
	 * Gets the total number of buffers.
	 *
	 * @return total number of buffer frames.
	 */
	public int getNumBuffers() {
		return this.NUMBUF;
	};

	/**
	 * Gets the total number of unpinned buffer frames.
	 *
	 * @return total number of unpinned buffer frames.
	 */
	public int getNumUnpinnedBuffers() {
		return 1;
	};

}
