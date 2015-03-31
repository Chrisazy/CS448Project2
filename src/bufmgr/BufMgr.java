/* ... */

package bufmgr;

import static global.SystemDefs.JavabaseDB;
import global.GlobalConst;
import global.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import chainexception.ChainException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

public class BufMgr implements GlobalConst {

	private class FrameDescriptor {

		public PageId page_number = null;
		public int pin_count = 0;
		public boolean dirtybit = false;

	}

	public static class NoAvailableFramesException extends ChainException {

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

	public int get(PageId pageNum) {
		int index = hash(pageNum.pid);
		ArrayList<Integer> bucket = tab[index];
		for (int i = 0; i < bucket.size(); i++) {
			int fdid = bucket.get(i);
			FrameDescriptor fd = bufDescr[fdid];
			if (fd.page_number.pid == pageNum.pid)
				return fdid;
		}
		return -1;
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
	 * @throws NoAvailableFramesException
	 */
	public void pinPage(PageId pin_pgid, Page page, boolean emptyPage)
			throws InvalidPageNumberException, FileIOException, IOException,
			NoAvailableFramesException {
		int fdid = get(pin_pgid);
		FrameDescriptor fd;
		if (fdid != -1) {
			fd = bufDescr[fdid];
			fd.pin_count++;
		} else {
			fdid = getReplacement();
			if (fdid == -1)
				throw new NoAvailableFramesException();
			fd = bufDescr[fdid];
			if (fd.page_number != null) {
				if (fd.dirtybit) {
					Page p = (new Page());
					p.setpage(bufpool[fdid]);
					JavabaseDB.write_page(fd.page_number, p);
				}
				remove(fd.page_number);
			}
			fd.page_number = pin_pgid;
			fd.dirtybit = false;
			fd.pin_count = 1;
			JavabaseDB.read_page(pin_pgid, page);
			for (int i = 0; i < page.getpage().length; i++) {
				bufpool[fdid][i] = page.getpage()[i];
			}
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
	 * @param pageNum
	 *            page number in the minibase.
	 * @param dirty
	 *            the dirty bit of the frame
	 * @throws ChainException
	 */
	public void unpinPage(PageId pageNum, boolean dirty) throws ChainException {
		int fdid = get(pageNum);
		FrameDescriptor fd = bufDescr[fdid];
		if (fd.pin_count == 0)
			throw new ChainException();
		fd.pin_count--;
		fd.dirtybit = dirty || fd.dirtybit;
		if (fd.pin_count == 0) {
			lruQueue.push(fdid);
		}
	}

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
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 * @throws InvalidRunSizeException
	 * @throws OutOfSpaceException
	 */
	public PageId newPage(Page firstpage, int howmany)
			throws OutOfSpaceException, InvalidRunSizeException,
			InvalidPageNumberException, FileIOException, DiskMgrException,
			IOException {
		PageId pageId = new PageId();
		System.out.println("Page id in newPage() " + pageId.pid);

		JavabaseDB.allocate_page(pageId, howmany);
		try {
			pinPage(pageId, firstpage, true);
		} catch (NoAvailableFramesException e) {
			// No available frames, so deallocate those pages from just now
			JavabaseDB.deallocate_page(pageId, howmany);
			return null;
		}
		return pageId;
	};

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 *
	 * @param globalPageId
	 *            the page number in the data base.
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 * @throws InvalidRunSizeException
	 */
	public void freePage(PageId globalPageId) throws InvalidRunSizeException,
			InvalidPageNumberException, FileIOException, DiskMgrException,
			IOException {
		JavabaseDB.deallocate_page(globalPageId);
	};

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 *
	 * @param pageid
	 *            the page number in the database.
	 * @throws IOException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 */
	public void flushPage(PageId pageid) throws InvalidPageNumberException,
			FileIOException, IOException {
		int fdid = get(pageid);
		if (fdid == -1)
			return;
		FrameDescriptor fd = bufDescr[fdid];
		if (fd.page_number != null) {
			Page p = (new Page());
			p.setpage(bufpool[fdid]);
			JavabaseDB.write_page(fd.page_number, p);
		} else {
			return;
		}
	}

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
