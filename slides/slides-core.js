/* ============================================================
   SLIDES CORE JS — Navigation & Fullscreen
   Big Data Course

   Cara pakai:
   <script src="slides-core.js"><\/script>
   Taruh sebelum </body>, setelah semua <section class="slide">.
   Auto-init: tidak perlu konfigurasi tambahan.
   ============================================================ */

/* === SLIDE PRESENTER (SP) === */
class SP {
    constructor() {
        this.s = document.querySelectorAll('.slide');
        this.c = 0;
        this.obs();
        this.key();
        this.touch();
        this.prog();
        this.dots();
        this.s[0].classList.add('visible');
    }

    obs() {
        const o = new IntersectionObserver(e => {
            e.forEach(en => {
                if (en.isIntersecting) {
                    en.target.classList.add('visible');
                    const i = [...this.s].indexOf(en.target);
                    if (i !== -1) { this.c = i; this.ud(); this.up(); }
                }
            });
        }, { threshold: .35 });
        this.s.forEach(s => o.observe(s));
    }

    key() {
        document.addEventListener('keydown', e => {
            if (['ArrowDown', ' ', 'PageDown', 'ArrowRight'].includes(e.key)) {
                e.preventDefault(); this.go(this.c + 1);
            } else if (['ArrowUp', 'PageUp', 'ArrowLeft'].includes(e.key)) {
                e.preventDefault(); this.go(this.c - 1);
            } else if (e.key === 'Home') {
                e.preventDefault(); this.go(0);
            } else if (e.key === 'End') {
                e.preventDefault(); this.go(this.s.length - 1);
            }
        });
    }

    touch() {
        let y = 0, t = 0;
        document.addEventListener('touchstart', e => {
            y = e.touches[0].clientY; t = Date.now();
        }, { passive: true });
        document.addEventListener('touchend', e => {
            const d = y - e.changedTouches[0].clientY;
            if (Math.abs(d) > 50 && Date.now() - t < 500) {
                d > 0 ? this.go(this.c + 1) : this.go(this.c - 1);
            }
        }, { passive: true });
    }

    prog() { this.bar = document.getElementById('progressBar'); this.up(); }
    up() { if (this.bar) this.bar.style.width = ((this.c + 1) / this.s.length * 100) + '%'; }

    dots() {
        const c = document.getElementById('navDots');
        if (!c) return;
        this.s.forEach((_, i) => {
            const d = document.createElement('button');
            d.className = 'nav-dot' + (i === 0 ? ' active' : '');
            d.addEventListener('click', () => this.go(i));
            c.appendChild(d);
        });
        this.d = c.querySelectorAll('.nav-dot');
    }

    ud() { if (this.d) this.d.forEach((d, i) => d.classList.toggle('active', i === this.c)); }

    go(i) {
        if (i < 0 || i >= this.s.length) return;
        this.c = i;
        this.s[i].scrollIntoView({ behavior: 'smooth' });
        this.ud();
        this.up();
    }
}

/* === FULLSCREEN TOGGLE === */
function toggleFS() {
    if (!document.fullscreenElement) {
        document.documentElement.requestFullscreen().catch(() => {});
    } else {
        document.exitFullscreen();
    }
}

document.addEventListener('fullscreenchange', () => {
    const ic = document.getElementById('fsIcon');
    if (!ic) return;
    if (document.fullscreenElement) {
        ic.innerHTML = '<path d="M8 3v3a2 2 0 01-2 2H3m18 0h-3a2 2 0 01-2-2V3m0 18v-3a2 2 0 012-2h3M3 16h3a2 2 0 012 2v3"/>';
    } else {
        ic.innerHTML = '<path d="M8 3H5a2 2 0 00-2 2v3m18 0V5a2 2 0 00-2-2h-3m0 18h3a2 2 0 002-2v-3M3 16v3a2 2 0 002 2h3"/>';
    }
});

document.addEventListener('keydown', e => {
    if (e.key === 'f' || e.key === 'F') {
        if (e.target.tagName !== 'INPUT' && e.target.tagName !== 'TEXTAREA') toggleFS();
    }
});

/* === AUTO INIT === */
new SP();
