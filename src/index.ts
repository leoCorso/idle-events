import { fromEvent, merge, Observable, of, startWith, Subject, Subscription, switchMap, takeUntil, tap, timer } from 'rxjs';

/**
 * Class that starts idle count and exposes events when idle is detected or idling stops
 * @example
 * ```typescript
 * const idleEvents = new IdleEvents(5000);
 * idleEvents.isIdle.subscribe(() => console.log('User is idle'));
 * idleEvents.idleStopped.subscribe(() => console.log('User is no longer idle'));
 * ```
 */
export class IdleEvents {
    protected element: HTMLElement; // The element to listen for idle events on
    protected idleTime: number; // Time in milliseconds after which the element is idled

    protected mainSubscription: Subscription | null = null;

    protected isIdle$ = new Subject<void>(); // Event emits after user is idle for specified time
    protected idleStopped$ = new Subject<void>(); // Event emits when user interupts idle timer

    /** Exposes the isIdle event as an observable that can be subscribed to when the user is idle for the specified time period */
    public isIdle = this.isIdle$.asObservable();
    
    /** Exposes the idleStopped event as an observable that can be subscribed to when the user interrupts the idle timer */
    public idleStopped = this.idleStopped$.asObservable();

    private readonly activityEvents$ = merge(
        fromEvent(window.document.body, 'mousemove'),
        fromEvent(window.document.body, 'mousedown'),
        fromEvent(window.document.body, 'mousewheel'),
        fromEvent(window.document.body, 'DOMMouseScroll'),
        fromEvent(window.document.body, 'keydown'),
        fromEvent(window.document.body, 'touchstart'),
        fromEvent(window.document.body, 'touchmove'),
        fromEvent(window.document.body, 'MSPointerDown'),
        fromEvent(window.document.body, 'MSPointerMove'),
        fromEvent(window.document.body, 'wheel'),
    );

    constructor(element: HTMLElement | undefined, idleTime: number) {
        this.element = element ?? document.body;
        this.idleTime = idleTime;
        this.startTimer();
    }

    protected startTimer(): void {
        this.mainSubscription = merge(
            this.activityEvents$.pipe( // Restarts timer
                tap(() => this.idleStopped$.next())
            ),
            of(null) // Start timer immediatley
        ).pipe(
            switchMap(() => timer(this.idleTime))
        )
        .subscribe(() => this.isIdle$.next());
    }
    /** Destroys the current idle timer */
    public destroyTimer(): void {
        if (this.mainSubscription) {
            this.mainSubscription.unsubscribe();
            this.mainSubscription = null;
        }
    }
    /** Resets the idle timer, stopping the current timer and starting a new one */
    public resetTimer(): void {
        if(this.mainSubscription) {
            this.destroyTimer();
        }
        this.startTimer();
    }
}