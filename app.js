  let bookmarkIds = new Set();
  let allBibleVerses = [];
  let allSourceRows = [];

  // Fuse.js search runs across a small pool of background workers so it can
  // never block the main thread. It's a pool rather than a single worker
  // because the app now auto-loads ~30 separate source-text collections
  // (Qur'an, six Hadith books, eight Hindu texts including the 15MB
  // Mahabharata, eight Buddhist texts, six Catholic/Apocrypha texts) — a
  // single worker searching all of them one after another is what made an
  // "All traditions" query take several seconds once enough texts were
  // loaded. Splitting the ~30 source-text collections across N workers and
  // searching them concurrently cuts that to roughly 1/N of the wall time
  // for the same results (see loadSourceIntoWorkers / handlePoolMessage).
  const SEARCH_POOL_SIZE = Math.min(6, Math.max(2, (navigator.hardwareConcurrency || 4)));
  const MERGE_TOP_CAP = 30; // matches search-worker.js's own TOP_CAP
  let searchWorkers = [];
  let bibleIndexReady = false;
  let sourceIndexReady = false;
  let searchReqSeq = 0;
  let latestSearchReqId = 0;
  const EMPTY_MATCHES = { total: 0, top: [] };
  let pendingOriginalSource = EMPTY_MATCHES;

  function initSearchWorkers() {
    if (typeof Worker === 'undefined') return; // no Worker support — search panels degrade to empty, non-fatal
    try {
      searchWorkers = Array.from({ length: SEARCH_POOL_SIZE }, (_, i) => {
        const w = new Worker('./search-worker.js');
        w.onmessage = (e) => handlePoolMessage(i, e);
        w.onerror = (err) => handlePoolWorkerError(i, err);
        return w;
      });
    } catch (err) {
      // A Worker() constructor failure here (restrictive CSP, resource limits,
      // some embedded-webview quirk) must never take the rest of app.js down
      // with it — this call runs before everything else in the file, so an
      // uncaught throw here would silently break the whole app, not just
      // search. Degrade the same way "no Worker support" already does.
      console.warn('TEP: failed to start search workers, search will be unavailable:', err);
      searchWorkers = [];
    }
  }
  initSearchWorkers();

  const ENTRIES = [
    // NOTE: entries marked "VERIFY WORDING" quote from a non-Bible source text
    // where exact translation/edition wording should be checked against the
    // primary source before publishing.
    {
      claim: "Jesus was just a prophet, not God",
      religion: "Islam",
      keywords: ["jesus is just a prophet", "jesus a prophet", "muhammad greater than jesus", "jesus not divine islam", "isa prophet"],
      sourceQuote: { ref: "Qur'an 4:171", text: "Christ Jesus the son of Mary was (no more than) a messenger of Allah." }, // VERIFY WORDING
      note: "The Qur'an affirms Jesus as a prophet and messenger, but explicitly denies his divinity and sonship.",
      verses: [
        { ref: "John 1:1", text: "In the beginning was the Word, and the Word was with God, and the Word was God." },
        { ref: "John 8:58", text: "Jesus said unto them, Verily, verily, I say unto you, Before Abraham was, I am." },
        { ref: "Colossians 2:9", text: "For in him dwelleth all the fulness of the Godhead bodily." }
      ]
    },
    {
      claim: "Jesus wasn't actually crucified",
      religion: "Islam",
      keywords: ["jesus not crucified", "islam crucifixion", "made to appear", "swoon theory islam"],
      sourceQuote: { ref: "Qur'an 4:157", text: "...they killed him not, nor crucified him, but so it was made to appear unto them..." }, // VERIFY WORDING
      note: "Most Islamic scholars read this as denying the crucifixion happened at all, holding that Jesus was raised to heaven before it.",
      verses: [
        { ref: "1 Corinthians 15:3-4", text: "For I delivered unto you first of all that which I also received, how that Christ died for our sins according to the scriptures; And that he was buried, and that he rose again the third day according to the scriptures:" },
        { ref: "John 19:33-34", text: "But when they came to Jesus, and saw that he was dead already, they brake not his legs: But one of the soldiers with a spear pierced his side, and forthwith came there out blood and water." }
      ]
    },
    {
      claim: "Salvation on Judgment Day depends on whether one's good deeds outweigh the bad on the scales",
      religion: "Islam",
      keywords: ["scales of deeds islam", "weighing of good and bad deeds", "mizan judgment day", "islam salvation by works"],
      sourceQuote: { ref: "Qur'an 23:102-103", text: "Then those whose balance (of good deeds) is heavy,- they will attain salvation: But those whose balance is light, will be those who have lost their souls, in Hell will they abide." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" },
        { ref: "Galatians 2:16", text: "Knowing that a man is not justified by the works of the law, but by the faith of Jesus Christ, even we have believed in Jesus Christ, that we might be justified by the faith of Christ, and not by the works of the law: for by the works of the law shall no flesh be justified." }
      ]
    },
    {
      claim: "The Torah and Gospel were corrupted by their followers, so the Qur'an corrects and supersedes them",
      religion: "Islam",
      keywords: ["tahrif corruption of scripture", "quran corrects the bible", "islam bible corrupted", "quran supersedes torah gospel"],
      sourceQuote: { ref: "Qur'an 5:13", text: "But because of their breach of their covenant, We cursed them, and made their hearts grow hard; they change the words from their (right) places and forget a good part of the message that was sent them..." },
      note: "This is the doctrine of tahrif (scriptural corruption), commonly invoked to explain differences between the Qur'an and the Bible.",
      verses: [
        { ref: "Psalm 119:89", text: "For ever, O LORD, thy word is settled in heaven." },
        { ref: "Matthew 24:35", text: "Heaven and earth shall pass away, but my words shall not pass away." },
        { ref: "1 Peter 1:25", text: "But the word of the Lord endureth for ever. And this is the word which by the gospel is preached unto you." }
      ]
    },
    {
      claim: "Jesus is Michael the Archangel, a created being",
      religion: "Jehovah's Witnesses",
      keywords: ["jesus is michael the archangel", "jehovah witness jesus created", "jesus not god jw", "arian jw"],
      positionSummary: "Official Jehovah's Witness teaching holds that Jesus pre-existed as the archangel Michael, God's first and greatest creation, rather than being eternally God himself.",
      verses: [
        { ref: "John 1:1", text: "In the beginning was the Word, and the Word was with God, and the Word was God." },
        { ref: "John 1:3", text: "All things were made by him; and without him was not any thing made that was made." },
        { ref: "Colossians 1:16", text: "For by him were all things created, that are in heaven, and that are in earth, visible and invisible, whether they be thrones, or dominions, or principalities, or powers: all things were created by him, and for him:" }
      ]
    },
    {
      claim: "Only 144,000 people go to heaven",
      religion: "Jehovah's Witnesses",
      keywords: ["144000", "144,000 heaven", "jehovah witness heaven", "anointed class"],
      positionSummary: "Jehovah's Witnesses teach that only a literal 144,000 \"anointed\" believers go to heaven to reign with Christ, while the rest of the faithful hope for eternal life on a paradise earth.",
      verses: [
        { ref: "Revelation 7:9", text: "After this I beheld, and, lo, a great multitude, which no man could number, of all nations, and kindreds, and people, and tongues, stood before the throne, and before the Lamb, clothed with white robes, and palms in their hands;" },
        { ref: "Revelation 7:10", text: "And cried with a loud voice, saying, Salvation to our God which sitteth upon the throne, and unto the Lamb." },
        { ref: "John 14:2-3", text: "In my Father's house are many mansions: if it were not so, I would have told you. I go to prepare a place for you. And if I go and prepare a place for you, I will come again, and receive you unto myself; that where I am, there ye may be also." }
      ]
    },
    {
      claim: "The wicked will be annihilated, not tormented forever in hell",
      religion: "Jehovah's Witnesses",
      keywords: ["annihilationism jw", "hell is the grave", "jehovah witness no eternal torment", "wicked cease to exist"],
      positionSummary: "Jehovah's Witnesses teach that hell (Sheol/Hades) is simply mankind's common grave, a state of unconscious non-existence, and that the unrepentant wicked will ultimately be annihilated — ceasing to exist entirely — rather than suffering conscious eternal torment.",
      verses: [
        { ref: "Matthew 25:46", text: "And these shall go away into everlasting punishment: but the righteous into life eternal." },
        { ref: "Revelation 14:11", text: "And the smoke of their torment ascendeth up for ever and ever: and they have no rest day nor night, who worship the beast and his image, and whosoever receiveth the mark of his name." },
        { ref: "Revelation 20:10", text: "And the devil that deceived them was cast into the lake of fire and brimstone... and shall be tormented day and night for ever and ever." }
      ]
    },
    {
      claim: "Christ returned invisibly in 1914 to begin ruling as heavenly king",
      religion: "Jehovah's Witnesses",
      keywords: ["1914 invisible return jw", "jehovah witness christ already returned", "last days began 1914", "invisible presence parousia"],
      positionSummary: "Jehovah's Witnesses teach that Jesus Christ began an invisible heavenly rule as King in the year 1914, marking the start of the \"last days,\" rather than a still-future, visible, bodily return witnessed by all people.",
      verses: [
        { ref: "Matthew 24:27", text: "For as the lightning cometh out of the east, and shineth even unto the west; so shall also the coming of the Son of man be." },
        { ref: "Acts 1:11", text: "Which also said, Ye men of Galilee, why stand ye gazing up into heaven? this same Jesus, which is taken up from you into heaven, shall so come in like manner as ye have seen him go into heaven." },
        { ref: "Revelation 1:7", text: "Behold, he cometh with clouds; and every eye shall see him, and they also which pierced him: and all kindreds of the earth shall wail because of him." }
      ]
    },
    {
      claim: "God the Father has a physical body",
      religion: "Mormonism",
      keywords: ["god has a body mormon", "heavenly father physical body", "lds god embodied"],
      sourceQuote: { ref: "Doctrine and Covenants 130:22", text: "The Father has a body of flesh and bones as tangible as man's; the Son also; but the Holy Ghost has not a body of flesh and bones, but is a personage of Spirit." }, // VERIFY WORDING
      verses: [
        { ref: "John 4:24", text: "God is a Spirit: and they that worship him must worship him in spirit and in truth." },
        { ref: "Luke 24:39", text: "Behold my hands and my feet, that it is I myself: handle me, and see; for a spirit hath not flesh and bones, as ye see me have." }
      ]
    },
    {
      claim: "The Book of Mormon is a second testament alongside the Bible",
      religion: "Mormonism",
      keywords: ["book of mormon another testament", "lds scripture book of mormon", "additional scripture mormon"],
      sourceQuote: { ref: "2 Nephi 25:23", text: "For we labor diligently to write, to persuade our children, and also our brethren, to believe in Christ, and to be reconciled to God; for we know that it is by grace that we are saved, after all we can do." }, // VERIFY WORDING
      note: "Latter-day Saints hold the Book of Mormon as scripture alongside the Bible, both understood as testaments of Jesus Christ.",
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Galatians 1:8-9", text: "But though we, or an angel from heaven, preach any other gospel unto you than that which we have preached unto you, let him be accursed. As we said before, so say I now again, If any man preach any other gospel unto you than that ye have received, let him be accursed." }
      ]
    },
    {
      claim: "God the Father was once a mortal man who progressed to become God, and righteous humans can likewise become gods",
      religion: "Mormonism",
      keywords: ["eternal progression mormon", "as man is god once was", "lds exaltation to godhood", "god was once a man"],
      sourceQuote: { ref: "Doctrine and Covenants 132:20", text: "Then shall they be gods, because they have no end; therefore shall they be from everlasting to everlasting, because they continue; then shall they be above all, because all things are subject unto them." }, // VERIFY WORDING
      note: "Popularly summarized in the Lorenzo Snow couplet: \"As man now is, God once was; as God now is, man may become.\"",
      verses: [
        { ref: "Isaiah 43:10", text: "...before me there was no God formed, neither shall there be after me." },
        { ref: "Malachi 3:6", text: "For I am the LORD, I change not; therefore ye sons of Jacob are not consumed." },
        { ref: "Numbers 23:19", text: "God is not a man, that he should lie; neither the son of man, that he should repent: hath he said, and shall he not do it? or hath he spoken, and shall he not make it good?" }
      ]
    },
    {
      claim: "Living people can be baptized on behalf of the dead so the deceased may receive the ordinance vicariously",
      religion: "Mormonism",
      keywords: ["baptism for the dead mormon", "proxy baptism lds", "vicarious baptism dead", "temple baptism ancestors"],
      positionSummary: "Latter-day Saints practice baptism by proxy on behalf of deceased ancestors, citing 1 Corinthians 15:29 as scriptural precedent, so that the dead may still accept the ordinance in the spirit world.",
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" },
        { ref: "John 3:36", text: "He that believeth on the Son hath everlasting life: and he that believeth not the Son shall not see life; but the wrath of God abideth on him." },
        { ref: "Hebrews 9:12", text: "Neither by the blood of goats and calves, but by his own blood he entered in once into the holy place, having obtained eternal redemption for us." }
      ]
    },
    {
      claim: "All paths lead to the same God",
      religion: "Hinduism",
      keywords: ["all religions same god", "all paths lead to god", "hindu pluralism", "many paths one god"],
      sourceQuote: { ref: "Bhagavad Gita 4:11", text: "As men approach me, so I receive them; all paths lead to me." }, // VERIFY WORDING — varies significantly by translation
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." }
      ]
    },
    {
      claim: "The soul is reincarnated again and again based on karma",
      religion: "Hinduism",
      keywords: ["reincarnation", "karma rebirth", "hindu soul next life", "samsara"],
      sourceQuote: { ref: "Bhagavad Gita 2:22", text: "As a man casts off worn-out garments and puts on others that are new, so the embodied soul casts off worn-out bodies and enters into others that are new." }, // VERIFY WORDING
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" }
      ]
    },
    {
      claim: "The four castes originate from a divine ordinance, and one's duty is fixed by the caste into which they are born",
      religion: "Hinduism",
      keywords: ["caste system hindu", "varna dharma", "four castes gita", "hereditary duty caste"],
      sourceQuote: { ref: "Bhagavad Gita 4:13", text: "The system of four stations was created by Me, according to the distinction of Gunas and Karma. Though I am their creator, know Me to be non-agent and immutable." },
      note: "Gita 18:47 reinforces this: \"Better is one's own duty, though ill done, than the duty of another, though well-performed.\"",
      verses: [
        { ref: "Galatians 3:28", text: "There is neither Jew nor Greek, there is neither bond nor free, there is neither male nor female: for ye are all one in Christ Jesus." },
        { ref: "Acts 17:26", text: "And hath made of one blood all nations of men for to dwell on all the face of the earth, and hath determined the times before appointed, and the bounds of their habitation;" }
      ]
    },
    {
      claim: "The individual self (atman) is ultimately identical with the ultimate reality (Brahman)",
      religion: "Hinduism",
      keywords: ["atman is brahman", "tat tvam asi", "that art thou upanishad", "hindu monism self is god"],
      sourceQuote: { ref: "Chandogya Upanishad 440", text: "...this whole world has that as its soul. That is Reality. That is Atman (Soul). That art thou, Svetaketu." },
      verses: [
        { ref: "Isaiah 55:8-9", text: "For my thoughts are not your thoughts, neither are your ways my ways, saith the LORD. For as the heavens are higher than the earth, so are my ways higher than your ways, and my thoughts than your thoughts." },
        { ref: "Isaiah 46:9", text: "...I am God, and there is none else; I am God, and there is none like me," }
      ]
    },
    {
      claim: "Suffering comes from craving, and ending desire ends suffering",
      religion: "Buddhism",
      keywords: ["buddhism suffering desire", "craving causes suffering", "end of desire nirvana", "second noble truth"],
      sourceQuote: { ref: "Dhammapada, ch. 24 (Craving)", text: "From craving springs grief, from craving springs fear; for him who is wholly free from craving there is no grief, much less fear." }, // VERIFY WORDING/verse number
      note: "This reflects the Second and Third Noble Truths: craving (tanha) is the origin of suffering, and its cessation is the path to Nirvana.",
      verses: [
        { ref: "Matthew 11:28-29", text: "Come unto me, all ye that labour and are heavy laden, and I will give you rest. Take my yoke upon you, and learn of me; for I am meek and lowly in heart: and ye shall find rest unto your souls." },
        { ref: "John 10:10", text: "The thief cometh not, but for to steal, and to kill, and to destroy: I am come that they might have life, and that they might have it more abundantly." }
      ]
    },
    {
      claim: "There is no eternal, unchanging soul (anatta)",
      religion: "Buddhism",
      keywords: ["anatta", "no soul buddhism", "no self buddhist", "impermanence self"],
      positionSummary: "Buddhism teaches anatta (\"non-self\") — that what we call the self is a changing bundle of processes, with no permanent, unchanging soul underlying it.",
      verses: [
        { ref: "Ecclesiastes 3:11", text: "He hath made every thing beautiful in his time: also he hath set the world in their heart, so that no man can find out the work that God maketh from the beginning to the end." },
        { ref: "Psalm 42:1-2", text: "As the hart panteth after the water brooks, so panteth my soul after thee, O God. My soul thirsteth for God, for the living God: when shall I come and appear before God?" }
      ]
    },
    {
      claim: "Each person must strive for their own liberation; the Buddha is a teacher, not a savior",
      religion: "Buddhism",
      keywords: ["buddha only points the way", "no savior buddhism", "self-reliance liberation", "work out your own salvation buddhist"],
      sourceQuote: { ref: "Dhammapada 276", text: "You yourselves must do the work, the Realized Ones just show the way. Meditators practicing absorption are released from Māra's bonds." },
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" }
      ]
    },
    {
      claim: "The highest goal is Nirvana, the extinguishing of the self and all craving — not everlasting conscious life with a personal God",
      religion: "Buddhism",
      keywords: ["nirvana extinguishment", "nibbana highest happiness", "buddhist final goal", "cessation not eternal life"],
      sourceQuote: { ref: "Dhammapada 204", text: "Health is the ultimate blessing; contentment, the ultimate wealth; trust is the ultimate family; extinguishment, the ultimate happiness." },
      note: "Nibbana (\"extinguishment\") — release from the cycle of craving and rebirth — is presented here as the highest happiness attainable.",
      verses: [
        { ref: "John 17:3", text: "And this is life eternal, that they might know thee the only true God, and Jesus Christ, whom thou hast sent." },
        { ref: "Revelation 21:3-4", text: "...Behold, the tabernacle of God is with men, and he will dwell with them, and they shall be his people, and God himself shall be with them, and be their God. And God shall wipe away all tears from their eyes; and there shall be no more death, neither sorrow, nor crying..." }
      ]
    },
    {
      claim: "There's no evidence for God",
      religion: "Atheism",
      keywords: ["no evidence for god", "atheism proof god", "no proof god exists"],
      positionSummary: "A common atheist position holds that belief in God lacks sufficient empirical evidence, and that natural explanations account for the universe without needing a creator.",
      verses: [
        { ref: "Romans 1:20", text: "For the invisible things of him from the creation of the world are clearly seen, being understood by the things that are made, even his eternal power and Godhead; so that they are without excuse:" },
        { ref: "Psalm 19:1", text: "The heavens declare the glory of God; and the firmament sheweth his handywork." }
      ]
    },
    {
      claim: "Morality doesn't require God",
      religion: "Atheism",
      keywords: ["morality without god", "atheist ethics", "can you be good without god"],
      positionSummary: "Many atheists argue that moral behavior arises from evolved social instincts, empathy, and reason, and does not depend on belief in a deity.",
      verses: [
        { ref: "Romans 2:14-15", text: "For when the Gentiles, which have not the law, do by nature the things contained in the law, these, having not the law, are a law unto themselves: Which shew the work of the law written in their hearts, their conscience also bearing witness, and their thoughts the mean while accusing or else excusing one another;" }
      ]
    },
    {
      claim: "The universe can be explained by natural causes alone, without need of a Creator",
      religion: "Atheism",
      keywords: ["universe without god", "big bang no creator", "abiogenesis atheism", "natural causes not god"],
      positionSummary: "Many atheists hold that cosmological models such as the Big Bang, and the possibility of abiogenesis, show the universe and life can in principle be accounted for by natural law and chance, without requiring a supernatural first cause.",
      verses: [
        { ref: "Genesis 1:1", text: "In the beginning God created the heaven and the earth." },
        { ref: "Hebrews 11:3", text: "Through faith we understand that the worlds were framed by the word of God, so that things which are seen were not made of things which do appear." },
        { ref: "Psalm 33:6", text: "By the word of the LORD were the heavens made; and all the host of them by the breath of his mouth." }
      ]
    },
    {
      claim: "The existence of evil and suffering is incompatible with an all-powerful, all-good God",
      religion: "Atheism",
      keywords: ["problem of evil", "suffering disproves god", "atheism argument from evil", "omnipotent good god suffering"],
      positionSummary: "The classical \"problem of evil\" argues that the amount and severity of suffering in the world is difficult to reconcile with a God who is simultaneously all-powerful and perfectly good — if he could prevent evil and doesn't, he isn't good; if he would but can't, he isn't all-powerful.",
      verses: [
        { ref: "Genesis 3:17-19", text: "...cursed is the ground for thy sake; in sorrow shalt thou eat of it all the days of thy life... In the sweat of thy face shalt thou eat bread, till thou return unto the ground..." },
        { ref: "Romans 8:20-22", text: "For the creature was made subject to vanity, not willingly, but by reason of him who hath subjected the same in hope... For we know that the whole creation groaneth and travaileth in pain together until now." },
        { ref: "Revelation 21:4", text: "And God shall wipe away all tears from their eyes; and there shall be no more death, neither sorrow, nor crying, neither shall there be any more pain: for the former things are passed away." }
      ]
    },
    {
      claim: "No single religion has exclusive access to truth",
      religion: "Pluralism",
      keywords: ["all religions equally true", "no exclusive truth", "religious pluralism"],
      positionSummary: "Religious pluralism holds that multiple, even conflicting, religious traditions can each contain genuine (if partial) truth and lead toward the same ultimate reality.",
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Sincerity matters more than which religion you follow",
      religion: "Pluralism",
      keywords: ["sincere belief enough", "sincerity over doctrine", "as long as you're sincere"],
      positionSummary: "A common pluralist view is that sincere devotion within any tradition is what matters, rather than the specific content of what is believed.",
      verses: [
        { ref: "Matthew 7:21-23", text: "Not every one that saith unto me, Lord, Lord, shall enter into the kingdom of heaven; but he that doeth the will of my Father which is in heaven. Many will say to me in that day, Lord, Lord, have we not prophesied in thy name? and in thy name have cast out devils? and in thy name done many wonderful works? And then will I profess unto them, I never knew you: depart from me, ye that work iniquity." }
      ]
    },
    {
      claim: "All major religions are different paths up the same mountain, teaching the same essential truths",
      religion: "Pluralism",
      keywords: ["paths up the same mountain", "perennialism", "all religions same essential truth", "transcendent unity of religions"],
      positionSummary: "Perennialist pluralism holds that beneath surface differences in ritual and doctrine, the major world religions converge on the same core spiritual and ethical truths, like different paths up a single mountain toward the same summit.",
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Isaiah 45:5", text: "I am the LORD, and there is none else, there is no God beside me..." }
      ]
    },
    {
      claim: "Insisting that only one religion is true is arrogant and the cause of religious conflict",
      religion: "Pluralism",
      keywords: ["exclusivism arrogant", "religious conflict caused by exclusivity", "intolerant to claim one true religion"],
      positionSummary: "A common pluralist objection holds that exclusive truth-claims are inherently intolerant, and that much religious conflict throughout history stems from this insistence rather than from any one religion's specific content.",
      verses: [
        { ref: "1 Peter 3:15", text: "But sanctify the Lord God in your hearts: and be ready always to give an answer to every man that asketh you a reason of the hope that is in you with meekness and fear:" },
        { ref: "Ephesians 4:15", text: "But speaking the truth in love, may grow up into him in all things, which is the head, even Christ:" }
      ]
    },
    {
      claim: "Humans determine their own values without appeal to the supernatural",
      religion: "Humanism",
      keywords: ["secular humanism values", "human-centered ethics", "no supernatural morality"],
      positionSummary: "Secular Humanism (cf. Humanist Manifesto III) holds that ethical values are derived from human experience and reason, without reference to a divine lawgiver.",
      verses: [
        { ref: "Jeremiah 10:23", text: "O LORD, I know that the way of man is not in himself: it is not in man that walketh to direct his steps." },
        { ref: "Proverbs 3:5-6", text: "Trust in the LORD with all thine heart; and lean not unto thine own understanding. In all thy ways acknowledge him, and he shall direct thy paths." }
      ]
    },
    {
      claim: "Reason and science are sufficient guides for life",
      religion: "Humanism",
      keywords: ["reason and science enough", "humanist rationalism", "science as guide for life"],
      positionSummary: "Humanists generally hold that reason, evidence, and scientific inquiry — not revelation — are the most reliable guides to truth and to living well.",
      verses: [
        { ref: "Proverbs 1:7", text: "The fear of the LORD is the beginning of knowledge: but fools despise wisdom and instruction." }
      ]
    },
    {
      claim: "Human dignity and rights derive from our shared humanity, not from being created in God's image",
      religion: "Humanism",
      keywords: ["human dignity secular", "rights from humanity not god", "humanist dignity without imago dei"],
      positionSummary: "Secular Humanism grounds human dignity and rights in our common humanity and capacity for reason and empathy, rather than in being made in the image of a Creator.",
      verses: [
        { ref: "Genesis 1:27", text: "So God created man in his own image, in the image of God created he him; male and female created he them." },
        { ref: "Psalm 8:4-5", text: "What is man, that thou art mindful of him? and the son of man, that thou visitest him? For thou hast made him a little lower than the angels, and hast crowned him with glory and honour." }
      ]
    },
    {
      claim: "This present life is the only one we have, so meaning must be found and created within it",
      religion: "Humanism",
      keywords: ["this life is all there is", "humanist manifesto no afterlife", "secular meaning in this life only"],
      positionSummary: "Humanist Manifestos describe this present life as \"all and enough,\" holding that since there is no evidence of an afterlife, human beings should find and create meaning, joy, and ethical purpose within their one earthly life.",
      verses: [
        { ref: "1 Corinthians 15:19", text: "If in this life only we have hope in Christ, we are of all men most miserable." },
        { ref: "2 Corinthians 4:18", text: "While we look not at the things which are seen, but at the things which are not seen: for the things which are seen are temporal; but the things which are not seen are eternal." }
      ]
    },
    {
      claim: "Lucifer represents enlightenment and self-liberation, not evil",
      religion: "Luciferianism",
      keywords: ["luciferianism lightbringer", "lucifer enlightenment", "lucifer not satan"],
      positionSummary: "Some Luciferian belief systems frame \"Lucifer\" (literally \"light-bringer\") as a symbol of illumination, knowledge, and self-liberation from imposed authority, rather than as a literal evil being. This varies significantly between individual adherents and groups — there is no single central text or authority.",
      verses: [
        { ref: "Isaiah 14:12-15", text: "How art thou fallen from heaven, O Lucifer, son of the morning! how art thou cut down to the ground, which didst weaken the nations! For thou hast said in thine heart, I will ascend into heaven, I will exalt my throne above the stars of God: I will sit also upon the mount of the congregation, in the sides of the north: I will ascend above the heights of the clouds; I will be like the most High. Yet thou shalt be brought down to hell, to the sides of the pit." },
        { ref: "2 Corinthians 11:14", text: "And no marvel; for Satan himself is transformed into an angel of light." }
      ]
    },
    {
      claim: "'Ye shall be as gods' is liberation, not deception",
      religion: "Luciferianism",
      keywords: ["ye shall be as gods", "as gods knowing good and evil", "genesis serpent liberation"],
      positionSummary: "Some Luciferian readings treat the serpent's offer in Genesis 3 as an invitation to enlightenment and self-determination, rather than as a lie that led to humanity's fall.",
      verses: [
        { ref: "Genesis 3:4-5", text: "And the serpent said unto the woman, Ye shall not surely die: For God doth know that in the day ye eat thereof, then your eyes shall be opened, and ye shall be as gods, knowing good and evil." },
        { ref: "Romans 6:23", text: "For the wages of sin is death; but the gift of God is eternal life through Jesus Christ our Lord." }
      ]
    },
    {
      claim: "God forbade the knowledge of good and evil out of jealousy, fearing humanity would become his equal",
      religion: "Luciferianism",
      keywords: ["god jealous of humanity", "become as one of us genesis", "luciferian reading of eden", "god fears competition"],
      sourceQuote: { ref: "Genesis 3:22", text: "And the LORD God said, Behold, the man is become as one of us, to know good and evil: and now, lest he put forth his hand, and take also of the tree of life, and eat, and live for ever:" },
      note: "Some Luciferian readings interpret God's exclusion of Adam and Eve from Eden not as a just response to disobedience, but as anxious self-protection by a threatened deity guarding forbidden knowledge and immortality for himself alone.",
      verses: [
        { ref: "James 1:13", text: "Let no man say when he is tempted, I am tempted of God: for God cannot be tempted with evil, neither tempteth he any man." },
        { ref: "Psalm 145:8-9", text: "The LORD is gracious, and full of compassion; slow to anger, and of great mercy. The LORD is good to all: and his tender mercies are over all his works." }
      ]
    },
    {
      claim: "Individual will and self-determination are sovereign; no external law or deity should bind the self",
      religion: "Luciferianism",
      keywords: ["sovereign self luciferian", "self as highest authority", "reject external divine law", "individual will supreme"],
      positionSummary: "A recurring Luciferian theme treats the sovereign, self-determining individual will as the highest authority, rejecting submission to any external divine law as a form of bondage to be thrown off.",
      verses: [
        { ref: "Proverbs 14:12", text: "There is a way which seemeth right unto a man, but the end thereof are the ways of death." },
        { ref: "James 4:7", text: "Submit yourselves therefore to God. Resist the devil, and he will flee from you." }
      ]
    },
    {
      claim: "All religions worship the same God under different names",
      religion: "Freemasonry",
      keywords: ["great architect of the universe", "freemasonry all religions same god", "masonic god"],
      positionSummary: "Masonic ritual refers to God as the \"Great Architect of the Universe,\" a deliberately generic title intended to be acceptable to members from different religious backgrounds, which in practice treats the specific identity of God as secondary to shared moral fraternity.",
      verses: [
        { ref: "Exodus 20:3", text: "Thou shalt have none other gods before me." },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Freemasonry isn't a religion, just a moral fraternity",
      religion: "Freemasonry",
      keywords: ["freemasonry not a religion", "masonic fraternity morality", "is freemasonry a religion"],
      positionSummary: "Mainstream Masonic bodies officially describe Freemasonry as a fraternal, charitable organization built around shared moral teaching and symbolism, not a religion or a path to salvation — though it does require belief in a \"Supreme Being.\"",
      verses: [
        { ref: "James 1:22", text: "But be ye doers of the word, and not hearers only, deceiving your own selves." }
      ]
    },
    {
      claim: "The soul's immortality is taught through the allegory of Hiram Abiff being 'raised' from a symbolic death",
      religion: "Freemasonry",
      keywords: ["hiram abiff legend", "master mason raised allegory", "masonic third degree resurrection allegory"],
      positionSummary: "The Masonic third-degree (\"Master Mason\") ritual centers on the legend of Hiram Abiff, the Temple's builder, who is murdered and then symbolically \"raised\" — presented as an allegory teaching the immortality of the soul, distinct from any claim to an actual, historical resurrection.",
      verses: [
        { ref: "1 Corinthians 15:3-4", text: "For I delivered unto you first of all that which I also received, how that Christ died for our sins according to the scriptures; And that he was buried, and that he rose again the third day according to the scriptures:" },
        { ref: "John 11:25", text: "Jesus said unto her, I am the resurrection, and the life: he that believeth in me, though he were dead, yet shall he live:" }
      ]
    },
    {
      claim: "A morally upright life, summarized as being 'good and true,' is what makes a person acceptable to God",
      religion: "Freemasonry",
      keywords: ["masonic morality acceptable to god", "good and true mason", "celestial lodge above", "masonic works based acceptance"],
      positionSummary: "Masonic teaching emphasizes moral uprightness, brotherly love, and charitable works as what qualifies a soul for the \"Celestial Lodge above,\" without reference to salvation through Christ's atonement specifically.",
      verses: [
        { ref: "Isaiah 64:6", text: "But we are all as an unclean thing, and all our righteousnesses are as filthy rags; and we all do fade as a leaf; and our iniquities, like the wind, have taken us away." },
        { ref: "Romans 3:20", text: "Therefore by the deeds of the law there shall no flesh be justified in his sight: for by the law is the knowledge of sin." }
      ]
    },
    {
      claim: "The universe is not a separate creation but a mental image existing within the Mind of THE ALL",
      religion: "Gnosticism",
      keywords: ["hermetic principle of mentalism", "the all is mind", "the universe is mental", "kybalion mentalism"],
      sourceQuote: { ref: "The Kybalion, Ch. 2, para. 6", text: "\"THE ALL IS MIND; The Universe is Mental.\"--The Kybalion." },
      note: "The Kybalion's first Hermetic Principle, Mentalism, holds that the physical universe has no independent existence apart from THE ALL's mind — it is a mental creation, closer to a thought or dream than to a separate, freestanding work of craftsmanship.",
      verses: [
        { ref: "Genesis 1:1", text: "In the beginning God created the heaven and the earth." },
        { ref: "Hebrews 11:3", text: "Through faith we understand that the worlds were framed by the word of God, so that things which are seen were not made of things which do appear." }
      ]
    },
    {
      claim: "Good and evil are not opposites but two poles of the same thing, differing only in degree",
      religion: "Gnosticism",
      keywords: ["hermetic principle of polarity", "good and evil same thing", "opposites reconciled kybalion", "polarity good evil"],
      sourceQuote: { ref: "The Kybalion, Ch. 2, para. 16", text: "\"Good and Evil\" are but the poles of the same thing, and the Hermetist understands the art of transmuting Evil into Good, by means of an application of the Principle of Polarity." },
      note: "The Principle of Polarity treats moral opposites the same way it treats heat and cold — as varying degrees of a single underlying reality that can be shifted along a scale, rather than as a fixed, categorical distinction.",
      verses: [
        { ref: "Isaiah 5:20", text: "Woe unto them that call evil good, and good evil; that put darkness for light, and light for darkness; that put bitter for sweet, and sweet for bitter!" },
        { ref: "1 John 1:5", text: "This then is the message which we have heard of him, and declare unto you, that God is light, and in him is no darkness at all." }
      ]
    },
    {
      claim: "\"As above, so below\": the same laws govern the spiritual, mental, and physical planes alike",
      religion: "Gnosticism",
      keywords: ["as above so below", "hermetic principle of correspondence", "kybalion planes of correspondence"],
      sourceQuote: { ref: "The Kybalion, Ch. 2, para. 9", text: "\"As above, so below; as below, so above.\"--The Kybalion." },
      note: "The Principle of Correspondence lets a Hermetist reason directly from earthly, knowable patterns to divine, unknowable ones, treating Creator and creation as different degrees of the same continuous scale rather than as fundamentally distinct kinds of being.",
      verses: [
        { ref: "Isaiah 55:8-9", text: "For my thoughts are not your thoughts, neither are your ways my ways, saith the LORD. For as the heavens are higher than the earth, so are my ways higher than your ways, and my thoughts than your thoughts." },
        { ref: "Numbers 23:19", text: "God is not a man, that he should lie; neither the son of man, that he should repent: hath he said, and shall he not do it? or hath he spoken, and shall he not make it good?" }
      ]
    },
    {
      claim: "Masculine and feminine principles pervade all things, mentally and physically, on every plane of existence",
      religion: "Gnosticism",
      keywords: ["hermetic principle of gender", "mental gender kybalion", "masculine feminine principle everything"],
      sourceQuote: { ref: "The Kybalion, Ch. 13, para. 1", text: "\"Gender is in everything; everything has its Masculine and Feminine Principles; Gender manifests on all planes.\"--The Kybalion." },
      note: "Where the Kybalion universalizes gender into an abstract creative force running through all matter, mind, and energy — inanimate objects included — Genesis ties male and female to the deliberate creation of humankind specifically, in God's own image.",
      verses: [
        { ref: "Genesis 1:27", text: "So God created man in his own image, in the image of God created he him; male and female created he them." },
        { ref: "Genesis 5:2", text: "Male and female created he them; and blessed them, and called their name Adam, in the day when they were created." }
      ]
    },
    {
      claim: "The world was created through an impersonal alchemical process, not by a spoken divine command",
      religion: "Gnosticism",
      keywords: ["emerald tablet creation", "so was the world created", "hermetic cosmogony", "alchemical creation of the world"],
      sourceQuote: { ref: "Emerald Tablet, v. 12", text: "So was the world created." },
      note: "This line closes a description of an impersonal operation — separating earth from fire, ascending and descending, receiving \"the force of things superior and inferior\" — presented as the very mechanism by which the world came to be, rather than the result of a personal being's decree.",
      verses: [
        { ref: "Genesis 1:3", text: "And God said, Let there be light: and there was light." },
        { ref: "Psalm 33:9", text: "For he spake, and it was done; he commanded, and it stood fast." }
      ]
    },
    {
      claim: "The Sun and Moon are the literal father and mother of the primordial substance from which the cosmos unfolds",
      religion: "Gnosticism",
      keywords: ["sun father moon mother hermetic", "emerald tablet sun moon", "hermetic celestial parentage"],
      sourceQuote: { ref: "Emerald Tablet, v. 4-5", text: "The Sun is its father, the Moon its mother, the wind hath carried it in its belly, the earth its nurse." },
      note: "The Tablet frames Sun and Moon not merely as luminaries but as literal progenitors in a genealogy — father and mother — of the substance from which all else descends.",
      verses: [
        { ref: "Genesis 1:16", text: "And God made two great lights; the greater light to rule the day, and the lesser light to rule the night: he made the stars also." },
        { ref: "Genesis 1:14", text: "And God said, Let there be lights in the firmament of the heaven to divide the day from the night; and let them be for signs, and for seasons, and for days, and years:" }
      ]
    },
    {
      claim: "Glory and deliverance from all obscurity come through mastering a hidden technique, not through grace",
      religion: "Gnosticism",
      keywords: ["emerald tablet glory obscurity", "hermetic gnosis technique salvation", "by this means glory of the world"],
      sourceQuote: { ref: "Emerald Tablet, v. 10", text: "By this means you shall have the glory of the whole world, and thereby all obscurity shall fly from you." },
      note: "The \"means\" in view is the operation the Tablet has just described — glory and the banishing of darkness are the payoff of correctly applying a technique, available to whoever masters the method, rather than something given.",
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "2 Corinthians 4:6", text: "For God, who commanded the light to shine out of darkness, hath shined in our hearts, to give the light of the knowledge of the glory of God in the face of Jesus Christ." }
      ]
    },
    {
      claim: "Hermes Trismegistus claims personal possession of the complete threefold philosophy of the whole world",
      religion: "Gnosticism",
      keywords: ["hermes trismegistus three parts wisdom", "emerald tablet thrice great", "hermetic claim to total wisdom"],
      sourceQuote: { ref: "Emerald Tablet, v. 14", text: "Hence I am called Hermes Trismegistus, having the three parts of the philosophy of the whole world." },
      note: "The Tablet ends with its speaker naming himself \"Thrice-Great\" precisely because he holds all three parts of the world's philosophy — a claim to total, self-attained mastery of cosmic wisdom.",
      verses: [
        { ref: "1 Corinthians 3:19", text: "For the wisdom of this world is foolishness with God. For it is written, He taketh the wise in their own craftiness." },
        { ref: "Proverbs 3:5-7", text: "Trust in the LORD with all thine heart; and lean not unto thine own understanding... Be not wise in thine own eyes." }
      ]
    },
    {
      claim: "Fire is venerated in worship as the son of Ahura Mazda, offered sacrifice and praise",
      religion: "Zoroastrianism",
      keywords: ["zoroastrian fire worship", "fire son of ahura mazda", "yasna fire hymn", "zoroastrianism sacred fire"],
      sourceQuote: { ref: "Yasna 0.2", text: "To Fire, the son of Ahura Mazda. To you, O Fire, son of Ahura Mazda. With propitiation, for worship, adoration, propitiation, and praise." },
      note: "The Yasna repeatedly addresses Fire directly as an object of worship and praise in its own right (see also Yasna 36, \"To Ahura and the Fire\"), not merely as a symbol used within worship of Ahura Mazda.",
      verses: [
        { ref: "Exodus 3:2-5", text: "And the angel of the LORD appeared unto him in a flame of fire out of the midst of a bush... And he said, Draw not nigh hither: put off thy shoes from off thy feet, for the place whereon thou standest is holy ground." },
        { ref: "Deuteronomy 4:24", text: "For the LORD thy God is a consuming fire, even a jealous God." }
      ]
    },
    {
      claim: "Guardian spirits (Fravashis) of the righteous have existed from eternity past and are invoked in worship",
      religion: "Zoroastrianism",
      keywords: ["fravashi zoroastrian", "guardian spirits existed from of old", "yasna fravashis", "pre-existent souls zoroastrianism"],
      sourceQuote: { ref: "Yasna 23.1", text: "I desire to approach with my praise those Fravashis which have existed from of old, the Fravashis of the houses, and of the villages, of the communities, and of the provinces..." },
      note: "Fravashis are held to have existed \"from of old\" — before the persons they belong to were even born — and are the object of dedicated praise and sacrifice (Yasna 23, 26), rather than beings created at conception.",
      verses: [
        { ref: "Psalm 139:13", text: "For thou hast possessed my reins: thou hast covered me in my mother's womb." },
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" }
      ]
    },
    {
      claim: "Worship includes actively cursing and 'smiting' a rival evil spirit, Angra Mainyu, alongside praising Ahura Mazda",
      religion: "Zoroastrianism",
      keywords: ["angra mainyu zoroastrian dualism", "smite the wicked spirit", "yasna cosmic dualism", "ahriman opposing god"],
      sourceQuote: { ref: "Yasna 27.1", text: "This is to render Him who is of all the greatest, our lord and master (even) Ahura Mazda. And this to smite the wicked Angra Mainyu, and to smite Aeshma of the bloody spear, and the Mazainya Daevas..." },
      note: "The liturgy pairs praise of Ahura Mazda with a formal act of \"smiting\" his opposite, Angra Mainyu, framing the two as opposing forces addressed together in the same breath of worship.",
      verses: [
        { ref: "Job 1:6-12", text: "Now there was a day when the sons of God came to present themselves before the LORD, and Satan came also among them... And the LORD said unto Satan, Behold, all that he hath is in thy power; only upon himself put not forth thine hand." },
        { ref: "Colossians 2:15", text: "And having spoiled principalities and powers, he made a shew of them openly, triumphing over them in it." }
      ]
    },
    {
      claim: "Good thoughts, good words, and good deeds, professed and accepted by the worshipper, constitute the substance of righteousness",
      religion: "Zoroastrianism",
      keywords: ["good thoughts good words good deeds", "zoroastrian ethical formula", "yasna righteousness", "zoroastrianism self-professed righteousness"],
      sourceQuote: { ref: "Yasna 0.4", text: "I praise good thoughts, good words, and good deeds and those that are to be thought, spoken, and done. I do accept all good thoughts, good words, and good deeds. I do renounce all evil thoughts, evil words, and evil deeds." },
      note: "Righteousness here is entered into by the worshipper's own profession and resolve — praising and accepting good thoughts, words, and deeds while renouncing evil ones — rather than received as something given from outside the self.",
      verses: [
        { ref: "Jeremiah 17:9", text: "The heart is deceitful above all things, and desperately wicked: who can know it?" },
        { ref: "Isaiah 64:6", text: "But we are all as an unclean thing, and all our righteousnesses are as filthy rags; and we all do fade as a leaf; and our iniquities, like the wind, have taken us away." }
      ]
    },
    {
      claim: "Initiation includes a mock death-and-rebirth ceremony, reportedly involving a coffin",
      religion: "Skull and Bones",
      keywords: ["skull and bones initiation", "skull and bones coffin ritual", "yale secret society death rebirth", "order 322 initiation"],
      positionSummary: "According to a widely-cited 1876 account by a rival Yale society that broke into the group's hall, initiates undergo a ceremony symbolizing death and rebirth into the order, reportedly involving lying in a coffin. Skull and Bones itself has never confirmed or denied specific initiation practices, maintaining strict secrecy.",
      verses: [
        { ref: "Romans 6:3-4", text: "Know ye not, that so many of us as were baptized into Jesus Christ were baptized into his death? Therefore we are buried with him by baptism into death: that like as Christ was raised up from the dead by the glory of the Father, even so we also should walk in newness of life." }
      ]
    },
    {
      claim: "A human skull is reportedly present during initiation as a memento mori symbol of mortality",
      religion: "Skull and Bones",
      keywords: ["skull and bones human skull ritual", "memento mori secret society", "order 322 skull symbol"],
      positionSummary: "The same 1876 account describes a human skull displayed during initiation rites, understood as a memento mori — a reminder of mortality meant to impress initiates with the gravity of their new obligations.",
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" },
        { ref: "Ecclesiastes 9:5", text: "For the living know that they shall die: but the dead know not any thing, neither have they any more a reward; for the memory of them is forgotten." }
      ]
    },
    {
      claim: "Initiates swear an oath of absolute, lifelong secrecy about the society's activities and membership",
      religion: "Skull and Bones",
      keywords: ["skull and bones secrecy oath", "yale secret society silence", "order 322 lifetime oath"],
      positionSummary: "Members are bound by a lifelong vow never to discuss the society's rites, symbols, or internal affairs with outsiders — a secrecy that extends even to acknowledging basic facts about meetings or membership.",
      verses: [
        { ref: "Luke 8:17", text: "For nothing is secret, that shall not be made manifest; neither any thing hid, that shall not be known and come abroad." },
        { ref: "John 18:20", text: "Jesus answered him, I spake openly to the world; I ever taught in the synagogue, and in the temple, whither the Jews always resort; and in secret have I said nothing." }
      ]
    },
    {
      claim: "Members form a lifelong network of mutual loyalty and advancement among a small elite",
      religion: "Skull and Bones",
      keywords: ["skull and bones elite network", "bonesmen loyalty", "old boy network yale secret society"],
      positionSummary: "Fifteen new members (\"the Fifteen\") are elected into the order each year, joining a lifelong network in which members are expected to give one another preference and support in business, politics, and other advancement.",
      verses: [
        { ref: "James 2:1", text: "My brethren, have not the faith of our Lord Jesus Christ, the Lord of glory, with respect of persons." },
        { ref: "Ephesians 5:11", text: "And have no fellowship with the unfruitful works of darkness, but rather reprove them." }
      ]
    },
    {
      claim: "God is not a personal Creator but an impersonal, unknowable Absolute Principle",
      religion: "Theosophy",
      keywords: ["theosophy impersonal god", "universal divine principle theosophy", "theosophy reject personal god", "blavatsky absolute"],
      sourceQuote: { ref: "The Key to Theosophy, Section 5", text: "In such a God we do not believe. We reject the idea of a personal, or an extra-cosmic and anthropomorphic God, who is but the gigantic shadow of man... We believe in a Universal Divine Principle, the root of ALL, from which all proceeds." },
      note: "Theosophy explicitly rejects the Biblical God of Moses as \"anthropomorphic\", substituting an impersonal Absolute that cannot be addressed, known, or related to as a person.",
      verses: [
        { ref: "Exodus 3:14", text: "And God said unto Moses, I AM THAT I AM: and he said, Thus shalt thou say unto the children of Israel, I AM hath sent me unto you." },
        { ref: "Genesis 1:1", text: "In the beginning God created the heaven and the earth." }
      ]
    },
    {
      claim: "Karma, an impersonal law of retributive justice, governs moral consequence — not forgiveness or atonement",
      religion: "Theosophy",
      keywords: ["theosophy karma law of retribution", "karma replaces forgiveness", "theosophical doctrine of karma"],
      sourceQuote: { ref: "The Key to Theosophy, Section 11", text: "Karma, the universal law of retributive justice." },
      note: "Every deed and even every sinful thought is repaid by impersonal law rather than forgiven — Theosophy explicitly rejects the Christian doctrine of atonement in favor of this self-executing moral bookkeeping.",
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Psalm 103:10", text: "He hath not dealt with us after our sins; nor rewarded us according to our iniquities." }
      ]
    },
    {
      claim: "The soul reincarnates through a long series of lives, strung together like pearls on a thread",
      religion: "Theosophy",
      keywords: ["theosophy reincarnation", "sutratma thread soul", "theosophical rebirth doctrine"],
      sourceQuote: { ref: "The Key to Theosophy, Section 9", text: "That which undergoes periodical incarnation is the Sutratma, which means literally the \"Thread Soul\"... because, like the pearls on a thread, so is the long series of human lives strung together on that one thread." },
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" }
      ]
    },
    {
      claim: "Prayer is replaced by \"Will-Prayer\" — an internal command of the will, not a petition addressed to God",
      religion: "Theosophy",
      keywords: ["theosophy will-prayer", "theosophy rejects prayer", "internal command instead of petition"],
      sourceQuote: { ref: "The Key to Theosophy, Section 5", text: "We do not [pray]. We act, instead of talking... we call it WILL-PRAYER, and it is rather an internal command than a petition." },
      note: "Rather than petitioning a personal God, the Theosophist directs an act of will inward, toward the esoteric \"Father which is in secret\" understood as an aspect of self rather than an addressable divine Person.",
      verses: [
        { ref: "Philippians 4:6", text: "Be careful for nothing; but in every thing by prayer and supplication with thanksgiving let your requests be made known unto God." },
        { ref: "Matthew 7:7", text: "Ask, and it shall be given you; seek, and ye shall find; knock, and it shall be opened unto you:" }
      ]
    },
    {
      claim: "\"Do what thou wilt shall be the whole of the Law\" — one's own will is the supreme moral principle",
      religion: "Thelema",
      keywords: ["do what thou wilt", "thelema law of will", "book of the law thelema ethics", "love is the law love under will"],
      sourceQuote: { ref: "The Book of the Law, I:40", text: "Do what thou wilt shall be the whole of the Law." },
      note: "This axiom, repeated throughout the text (III:60) and in its closing Comment, makes the individual's own will the highest and only law — there is, in its own words, \"no law beyond Do what thou wilt.\"",
      verses: [
        { ref: "Matthew 26:39", text: "And he went a little farther, and fell on his face, and prayed, saying, O my Father, if it be possible, let this cup pass from me: nevertheless not as I will, but as thou wilt." },
        { ref: "James 4:7", text: "Submit yourselves therefore to God. Resist the devil, and he will flee from you." }
      ]
    },
    {
      claim: "\"Every man and every woman is a star\" — each person is a unique, self-sufficient point of divine will",
      religion: "Thelema",
      keywords: ["every man and every woman is a star", "thelema divine individualism", "book of the law self divinity"],
      sourceQuote: { ref: "The Book of the Law, I:3", text: "Every man and every woman is a star." },
      note: "The declaration places each individual's own nature at the center of the cosmos, radically distinct from a view in which humanity is creaturely and finite before its Creator.",
      verses: [
        { ref: "Psalm 8:3-4", text: "When I consider thy heavens, the work of thy fingers, the moon and the stars, which thou hast ordained; What is man, that thou art mindful of him?" }
      ]
    },
    {
      claim: "Compassion for the weak is condemned as a vice; the strong are told to \"stamp down the wretched and the weak\"",
      religion: "Thelema",
      keywords: ["compassion is the vice of kings", "thelema rejects pity", "book of the law stamp down the weak"],
      sourceQuote: { ref: "The Book of the Law, II:21", text: "We have nothing with the outcast and the unfit: let them die in their misery. For they feel not. Compassion is the vice of kings: stamp down the wretched & the weak: this is the law of the strong: this is our law and the joy of the world." },
      verses: [
        { ref: "James 1:27", text: "Pure religion and undefiled before God and the Father is this, To visit the fatherless and widows in their affliction, and to keep himself unspotted from the world." },
        { ref: "Matthew 25:40", text: "And the King shall answer and say unto them, Verily I say unto you, Inasmuch as ye have done it unto one of the least of these my brethren, ye have done it unto me." }
      ]
    },
    {
      claim: "The text's speaker declares itself the only God: \"there is no other God than me\"",
      religion: "Thelema",
      keywords: ["there is no other god than me", "book of the law nuit hadit", "thelema exclusive deity claim"],
      sourceQuote: { ref: "The Book of the Law, I:21", text: "With the God & the Adorer I am nothing: they do not see me. They are as upon the earth; I am Heaven, and there is no other God than me, and my lord Hadit." },
      verses: [
        { ref: "Isaiah 45:5", text: "I am the LORD, and there is none else, there is no God beside me: I girded thee, though thou hast not known me:" }
      ]
    },
    {
      claim: "The material world was not made by the true God but by Ialdabaoth, a lesser, ignorant ruler born when the fallen aeon Sophia's light was swallowed into the Chaos",
      religion: "Gnosticism",
      keywords: ["ialdabaoth demiurge gnostic", "sophia fall into chaos", "gnosticism matter is evil"],
      sourceQuote: { ref: "Pistis Sophia (Horner, 1924), First Document", text: "The great power of light of face of lion swallowed down the powers of light in the Sophia, and it purged her light she having swallowed it, and her matter was cast out unto the Chaos. It became a Ruler of face of lion in the Chaos, whose one half became fire and whose other half became darkness, namely, Ialdabaoth." },
      verses: [
        { ref: "Genesis 1:31", text: "And God saw every thing that he had made, and, behold, it was very good. And the evening and the morning were the sixth day." },
        { ref: "John 1:3", text: "All things were made by him; and without him was not any thing made that was made." }
      ]
    },
    {
      claim: "Jesus stripped the Rulers of the Aeons of their power over Fate, astrology, and magic, so they can no longer control human destiny",
      religion: "Gnosticism",
      keywords: ["gnostic archons rulers of fate", "pistis sophia destiny astrology", "gnosticism rulers of the aeons"],
      sourceQuote: { ref: "Pistis Sophia (Horner, 1924), First Document", text: "I took away the third part from the power of the Rulers of all the Aeons; and... removed their Destiny with their Spheres... in order that they should not be able to prevail any more from this hour to accomplish their unlawful works, because that thou tookest away their power from them." },
      verses: [
        { ref: "Colossians 2:15", text: "And having spoiled principalities and powers, he made a shew of them openly, triumphing over them in it." },
        { ref: "Isaiah 45:7", text: "I form the light, and create darkness: I make peace, and create evil: I the LORD do all these things." }
      ]
    },
    {
      claim: "Salvation comes through secret mysteries and hidden knowledge (gnosis) that Jesus reveals only to an inner circle of disciples, not through faith alone",
      religion: "Gnosticism",
      keywords: ["gnosis secret knowledge salvation", "pistis sophia mysteries revealed", "gnosticism esoteric salvation"],
      sourceQuote: { ref: "Pistis Sophia (Horner, 1924), First Document", text: "Ye are happy ones beyond every man who is upon the earth, because I revealed unto you these mysteries. Verily verily I say to you, I shall complete you with every Pleroma from the mysteries of the inward part even unto the mysteries of the outward part; and I shall fill you with the spirit, that they should call you the spiritual ones, completed with every Pleroma." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "1 Corinthians 15:3-4", text: "For I delivered unto you first of all that which I also received, how that Christ died for our sins according to the scriptures; And that he was buried, and that he rose again the third day according to the scriptures:" }
      ]
    },
    {
      claim: "After his resurrection, Jesus spent eleven years secretly teaching his disciples esoteric mysteries far beyond anything recorded in the public Gospels",
      religion: "Gnosticism",
      keywords: ["jesus eleven years secret teaching", "pistis sophia post resurrection", "gnostic hidden gospel"],
      sourceQuote: { ref: "Pistis Sophia (Horner, 1924), First Document", text: "It happened after that Jesus rose out of those who are dead, and he spent eleven years speaking with his disciples, and teaching them only as far as the Places of the First precept, and as far as the Places of the First Mystery." },
      verses: [
        { ref: "Acts 1:3", text: "To whom also he shewed himself alive after his passion by many infallible proofs, being seen of them forty days, and speaking of the things pertaining to the kingdom of God:" },
        { ref: "2 Peter 1:16", text: "For we have not followed cunningly devised fables, when we made known unto you the power and coming of our Lord Jesus Christ, but were eyewitnesses of his majesty." }
      ]
    },
    {
      claim: "Salvation is reached by learning and knowing one's own inner divine nature — \"the man who hath Mind in him, let him learn to know that he himself is deathless\"",
      religion: "Gnosticism",
      keywords: ["corpus hermeticum know thyself deathless", "gnosis self knowledge salvation", "hermetic man is deathless"],
      sourceQuote: { ref: "Corpus Hermeticum (Mead, 1906), Libellus I, 21", text: "If then thou learnest that thou art thyself of Life and Light, and that thou happen'st to be out of them, thou shalt return again to Life... The man who hath Mind in him, let him learn to know that he himself is deathless." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "The highest goal of those who attain gnosis is to be made one with God — becoming Powers who are themselves in God",
      religion: "Gnosticism",
      keywords: ["corpus hermeticum made one with god", "gnosis deification", "hermetic union with god"],
      sourceQuote: { ref: "Corpus Hermeticum (Mead, 1906), Libellus I, 26", text: "This the good end for those who have gained Gnosis - to be made one with God." },
      verses: [
        { ref: "Isaiah 42:8", text: "I am the LORD: that is my name: and my glory will I not give to another, neither my praise to graven images." },
        { ref: "Numbers 23:19", text: "God is not a man, that he should lie; neither the son of man, that he should repent." }
      ]
    },
    {
      claim: "A soul that lives in vice is reborn into lower creatures — reincarnating downward through animal forms rather than facing a single final judgment",
      religion: "Gnosticism",
      keywords: ["corpus hermeticum reincarnation soul", "hermetic transmigration souls", "gnosticism soul reborn animal"],
      sourceQuote: { ref: "Corpus Hermeticum (Mead, 1906), Libellus X, 8", text: "If a soul on entering the body of a man persisteth in its vice, it neither tasteth deathlessness nor shareth in the Good; but speeding back again it turns into the path that leads to creeping things. This is the sentence of the vicious soul." },
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" }
      ]
    },
    {
      claim: "A secret magic book written by the god Thoth's own hand grants power to enchant heaven, earth, and the sea, and to understand the speech of animals — but taking it brings ruin on whoever possesses it",
      religion: "Gnosticism",
      keywords: ["book of thoth magic secret power", "setna magic book thoth", "egyptian book of thoth curse"],
      sourceQuote: { ref: "Setna and the Magic Book (Petrie, 1895), para. 1", text: "He heard that the magic book of Thoth, by which a man may enchant heaven and earth, and know the language of all birds and beasts, was buried in the cemetery of Memphis." },
      note: "An ancient Egyptian tale (not itself the book's contents, which are not preserved) about Prince Setna's quest for the legendary Book of Thoth — Ahura, guarding the tomb, warns him: \"Do not take this book; for it will bring trouble on you, as it has upon us.\"",
      verses: [
        { ref: "James 1:5", text: "If any man of you lack wisdom, let him ask of God, that giveth to all men liberally, and upbraideth not; and it shall be given him." },
        { ref: "Deuteronomy 18:10-12", text: "There shall not be found among you any one that... useth divination, or an observer of times, or an enchanter... For all that do these things are an abomination unto the LORD." }
      ]
    },
    {
      claim: "The gods themselves declare the dead king their own divine son and heir, enthroned beside them, by the power of ritual proclamation alone",
      religion: "Egyptian Occultism",
      keywords: ["pyramid texts divine sonship king", "nut geb declare king son", "egyptian occultism king becomes god"],
      sourceQuote: { ref: "The Pyramid Texts (Mercer, 1952), Utterance 1", text: "To say by Nut, the brilliant, the great: This is (my) son, (my) first born, N., opener of (my) womb... this is (my) beloved, with whom I have been satisfied." },
      note: "The oldest religious texts in the world (Old Kingdom, c. 2400-2300 BC), carved into pyramid walls to secure the dead king's ascension and divine sonship by ritual declaration.",
      verses: [
        { ref: "John 1:12-13", text: "But as many as received him, to them gave he power to become the sons of God, even to them that believe on his name: Which were born, not of blood, nor of the will of the flesh, nor of the will of man, but of God." },
        { ref: "Galatians 4:7", text: "Wherefore thou art no more a servant, but a son; and if a son, then an heir of God through Christ." }
      ]
    },
    {
      claim: "The dead king departs not dead but living, enthroned in the place of Osiris and commanding the living, as messengers of Ra arrive to summon him — resurrection achieved through the correct performance of the funerary ritual",
      religion: "Egyptian Occultism",
      keywords: ["pyramid texts king departs living", "ritual resurrection osiris throne", "egyptian occultism ascension ritual"],
      sourceQuote: { ref: "The Pyramid Texts (Mercer, 1952), Utterance 213", text: "O N., thou didst not depart dead; thou didst depart living, (so) thou sittest upon the throne of Osiris... thou commandest the living." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "John 11:25", text: "Jesus said unto her, I am the resurrection, and the life: he that believeth in me, though he were dead, yet shall he live:" }
      ]
    },
    {
      claim: "The dead scribe's own heart is weighed on the balance by Thoth and found righteous on its own merit, granting entry to paradise",
      religion: "Egyptian Occultism",
      keywords: ["book of the dead weighing of the heart", "thoth balance judgment egypt", "egyptian occultism heart righteous"],
      sourceQuote: { ref: "The Book of the Dead (Budge, 1895), Plate IV", text: "His heart is found righteous coming forth from the balance, and it hath not sinned against god or goddess. Thoth hath weighed it according to the decree uttered unto him by the company of the gods; and it is very true and righteous." },
      note: "The Papyrus of Ani's judgment scene — the deceased's own heart, weighed against the feather of Maat, must be found righteous unaided for the soul to pass into paradise.",
      verses: [
        { ref: "Isaiah 64:6", text: "But we are all as an unclean thing, and all our righteousnesses are as filthy rags; and we all do fade as a leaf; and our iniquities, like the wind, have taken us away." },
        { ref: "Romans 3:20", text: "Therefore by the deeds of the law there shall no flesh be justified in his sight: for by the law is the knowledge of sin." }
      ]
    },
    {
      claim: "A spell recited over the heart itself compels it not to testify against the deceased at judgment: \"my heart, my mother... may there be nothing to resist me at my judgment\"",
      religion: "Egyptian Occultism",
      keywords: ["book of the dead heart scarab spell", "silence my heart judgment", "egyptian occultism negative confession"],
      sourceQuote: { ref: "The Book of the Dead (Budge, 1895), Plate III, Ch. XXX", text: "My heart my mother, my heart my mother, my heart my coming into being! May there be nothing to resist me at my judgment; may there be no opposition to me from the Tchatcha; may there be no parting of thee from me in the presence of him who keepeth the scales!" },
      note: "Chapter XXX(B), traditionally inscribed on a heart-scarab amulet placed on the mummy — a spell whose entire purpose is to silence the heart so it cannot testify truthfully against its owner.",
      verses: [
        { ref: "Hebrews 4:13", text: "Neither is there any creature that is not manifest in his sight: but all things are naked and opened unto the eyes of him with whom we have to do." },
        { ref: "Jeremiah 17:10", text: "I the LORD search the heart, I try the reins, even to give every man according to his ways, and according to the fruit of his doings." }
      ]
    },
    {
      claim: "A boy medium stares into a vessel of oil while a magician invokes the gods by secret names, compelling them to answer questions through the child's vision",
      religion: "Egyptian Occultism",
      keywords: ["demotic magical papyrus vessel divination", "boy medium egyptian divination", "egyptian occultism child oracle"],
      sourceQuote: { ref: "The Demotic Magical Papyrus of London and Leiden (Griffith & Thompson, 1904-1909), Col. II", text: "You say to the boy 'Open your eyes'; when he opens his eyes and sees the light, you make him cry out, saying 'Grow, O light, come forth, O light, rise, O light, ascend...'" },
      note: "A 2nd/3rd-century AD Egyptian magical handbook — vessel- (or lamp-) divination through a child medium was one of its most common procedures.",
      verses: [
        { ref: "Deuteronomy 18:10-12", text: "There shall not be found among you any one that... useth divination, or an observer of times, or an enchanter, or a witch... For all that do these things are an abomination unto the LORD." },
        { ref: "1 Samuel 28:7", text: "Then said Saul unto his servants, Seek me a woman that hath a familiar spirit, that I may go to her, and inquire of her." }
      ]
    },
    {
      claim: "The magician invokes a universal, unseen creator-god by a chain of secret names — \"Osoronnophris, whom no man hath seen at any time\" — to compel him to answer and obey",
      religion: "Egyptian Occultism",
      keywords: ["headless one invocation pgm", "osoronnophris secret names magic", "egyptian occultism compel god"],
      sourceQuote: { ref: "Fragment of a Graeco-Egyptian Work upon Magic (Goodwin, 1852), Spell 4", text: "I call thee, the headless one, that didst create earth and heaven, that didst create night and day, thee the creator of light and darkness. Thou art Osoronnophris, whom no man hath seen at any time." },
      note: "From the only public-domain translation of a Greek Magical Papyrus (British Museum Papyrus XLVI Greek, c. 2nd-4th century AD) — one papyrus, not the full modern PGM corpus, which remains under copyright.",
      verses: [
        { ref: "Isaiah 40:13-14", text: "Who hath directed the Spirit of the LORD, or being his counsellor hath taught him? With whom took he counsel, and who instructed him...?" },
        { ref: "Exodus 20:7", text: "Thou shalt not take the name of the LORD thy God in vain; for the LORD will not hold him guiltless that taketh his name in vain." }
      ]
    },
    {
      claim: "A magician's power lies in \"words of power\" recited with exact pronunciation — correctness of speech itself, not the speaker's character, determines whether the magic succeeds",
      religion: "Egyptian Occultism",
      keywords: ["egyptian magic words of power pronunciation", "budge words of power correct speech", "egyptian occultism mechanistic magic"],
      sourceQuote: { ref: "Egyptian Magic (Budge, 1899), para. 5", text: "He uttered the words of power which he knew with correct pronunciation, and halted not in his speech, and was perfect both in giving the command and in saying the word." },
      verses: [
        { ref: "Matthew 6:7", text: "But when ye pray, use not vain repetitions, as the heathen do: for they think that they shall be heard for their much speaking." },
        { ref: "1 Samuel 16:7", text: "For the LORD seeth not as man seeth; for man looketh on the outward appearance, but the LORD looketh on the heart." }
      ]
    },
    {
      claim: "The Coffin Texts democratized the royal afterlife: spells once reserved for pharaohs alone were inscribed on ordinary coffins, letting any person who could afford one navigate the underworld's hazards by the same magic",
      religion: "Egyptian Occultism",
      keywords: ["coffin texts democratization afterlife", "middle kingdom afterlife spells", "egyptian occultism coffin spells"],
      positionSummary: "The Coffin Texts (Middle Kingdom, c. 2100-1700 BC) adapted and expanded the earlier royal-only Pyramid Texts into spells any non-royal person could have inscribed on their coffin, extending the promise of a secure afterlife — previously a privilege of kingship — to whoever could afford the ritual and the object. No public-domain English translation of the Coffin Texts exists (R. O. Faulkner's 1973-78 translation remains the standard, actively copyrighted edition), so this entry has no source text.",
      verses: [
        { ref: "Galatians 3:28", text: "There is neither Jew nor Greek, there is neither bond nor free, there is neither male nor female: for ye are all one in Christ Jesus." },
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." }
      ]
    },
    {
      claim: "The Book of Two Ways maps the underworld's twin roads of fire and water, showing the soul precisely which obstacles lie ahead and how to pass them safely",
      religion: "Egyptian Occultism",
      keywords: ["book of two ways underworld map", "egyptian occultism fire water paths", "coffin texts cosmography"],
      positionSummary: "Found painted on the floors of Middle Kingdom coffins alongside the Coffin Texts (of which it is technically a part, roughly spells 1029-1130), the Book of Two Ways is the oldest known map of the hereafter — a diagram of parallel paths of fire and water the soul must correctly navigate, each hazard labeled and countered by an accompanying spell. As a subset of the Coffin Texts, no public-domain English translation exists for this either.",
      verses: [
        { ref: "Psalm 23:4", text: "Yea, though I walk through the valley of the shadow of death, I will fear no evil: for thou art with me; thy rod and thy staff they comfort me." },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "\"Satan represents indulgence, instead of abstinence!\" — self-gratification is exalted over self-denial",
      religion: "LaVeyan Satanism",
      keywords: ["satanic bible nine statements", "satan represents indulgence", "laveyan satanism hedonism"],
      sourceQuote: { ref: "The Satanic Bible, The Nine Satanic Statements", text: "Satan represents indulgence, instead of abstinence!" },
      note: "The first of LaVey's Nine Satanic Statements, published by the Church of Satan as a summary of its philosophy.",
      verses: [
        { ref: "Galatians 5:16", text: "This I say then, Walk in the Spirit, and ye shall not fulfil the lust of the flesh." },
        { ref: "1 Peter 2:11", text: "Dearly beloved, I beseech you as strangers and pilgrims, abstain from fleshly lusts, which war against the soul;" }
      ]
    },
    {
      claim: "\"Satan represents kindness to those who deserve it, instead of love wasted on ingrates!\" — love is conditional on merit",
      religion: "LaVeyan Satanism",
      keywords: ["satanic bible kindness to those who deserve it", "laveyan satanism conditional love", "love wasted on ingrates"],
      sourceQuote: { ref: "The Satanic Bible, The Nine Satanic Statements", text: "Satan represents kindness to those who deserve it, instead of love wasted on ingrates!" },
      verses: [
        { ref: "Matthew 5:44", text: "But I say unto you, Love your enemies, bless them that curse you, do good to them that hate you, and pray for them which despitefully use you, and persecute you;" },
        { ref: "Luke 6:35", text: "But love ye your enemies, and do good, and lend, hoping for nothing again; and your reward shall be great, and ye shall be the children of the Highest: for he is kind unto the unthankful and to the evil." }
      ]
    },
    {
      claim: "\"Satan represents vengeance, instead of turning the other cheek!\"",
      religion: "LaVeyan Satanism",
      keywords: ["satanic bible vengeance", "satan represents vengeance", "laveyan satanism turning the other cheek"],
      sourceQuote: { ref: "The Satanic Bible, The Nine Satanic Statements", text: "Satan represents vengeance, instead of turning the other cheek!" },
      verses: [
        { ref: "Matthew 5:39", text: "But I say unto you, That ye resist not evil: but whosoever shall smite thee on thy right cheek, turn to him the other also." },
        { ref: "Romans 12:19", text: "Dearly beloved, avenge not yourselves, but rather give place unto wrath: for it is written, Vengeance is mine; I will repay, saith the Lord." }
      ]
    },
    {
      claim: "Man is \"just another animal,\" made vicious rather than virtuous by his claimed spiritual development",
      religion: "LaVeyan Satanism",
      keywords: ["satan represents man as just another animal", "laveyan satanism human nature", "satanic bible most vicious animal"],
      sourceQuote: { ref: "The Satanic Bible, The Nine Satanic Statements", text: "Satan represents man as just another animal, sometimes better, more often worse than those that walk on all-fours, who, because of his \"divine spiritual and intellectual development,\" has become the most vicious animal of all!" },
      verses: [
        { ref: "Genesis 1:27", text: "So God created man in his own image, in the image of God created he him; male and female created he them." },
        { ref: "Psalm 8:5", text: "For thou hast made him a little lower than the angels, and hast crowned him with glory and honour." }
      ]
    },
    {
      claim: "Set is a real, literal, non-physical intelligence who communicates directly with initiates — not merely a psychological symbol",
      religion: "Temple of Set",
      keywords: ["temple of set theistic satanism", "set literal entity aquino", "book of coming forth by night"],
      positionSummary: "The Temple of Set, founded in 1975 by Michael Aquino after he split from the Church of Satan, explicitly rejects Anton LaVey's purely symbolic/psychological use of \"Satan.\" Aquino claimed to have received a text, \"The Book of Coming Forth by Night,\" directly from Set, an actual non-physical intelligence identified with the ancient Egyptian deity Setekh.",
      verses: [
        { ref: "2 Corinthians 11:14", text: "And no marvel; for Satan himself is transformed into an angel of light." },
        { ref: "1 John 4:1", text: "Beloved, believe not every spirit, but try the spirits whether they are of God: because many false prophets are gone out into the world." }
      ]
    },
    {
      claim: "\"Xeper\" — the self-authored, self-directed pursuit of one's own becoming — is the Setian's central spiritual principle",
      religion: "Temple of Set",
      keywords: ["xeper setian philosophy", "temple of set self-becoming", "setian self-authored evolution"],
      positionSummary: "\"Xeper\" (an ancient Egyptian word meaning roughly \"I have come into being\") names the Temple of Set's core practice: treating one's own consciousness and spiritual evolution as fundamentally self-created and self-directed, rather than something received, revealed, or dependent on God.",
      verses: [
        { ref: "Jeremiah 10:23", text: "O LORD, I know that the way of man is not in himself: it is not in man that walketh to direct his steps." },
        { ref: "Proverbs 3:5-6", text: "Trust in the LORD with all thine heart; and lean not unto thine own understanding. In all thy ways acknowledge him, and he shall direct thy paths." }
      ]
    },
    {
      claim: "Set, not the Serpent's deception, is credited with first giving humanity self-aware consciousness, separating man from the animals",
      religion: "Temple of Set",
      keywords: ["set gift of consciousness", "temple of set genesis serpent reinterpreted", "isolate intelligence setian"],
      positionSummary: "Setian teaching credits Set with originally bestowing \"isolate intelligence\" — self-aware consciousness — on humanity, reframing the role the Genesis serpent plays in Eden as a gift of enlightenment rather than a deception leading to the Fall.",
      verses: [
        { ref: "Genesis 3:13", text: "And the LORD God said unto the woman, What is this that thou hast done? And the woman said, The serpent beguiled me, and I did eat." },
        { ref: "2 Corinthians 11:3", text: "But I fear, lest by any means, as the serpent beguiled Eve through his subtlety, so your minds should be corrupted from the simplicity that is in Christ." }
      ]
    },
    {
      claim: "Setians seek companionship and self-deification alongside Set, not submissive worship of him",
      religion: "Temple of Set",
      keywords: ["setian rejects worship", "temple of set self-deification", "companion of set not worshipper"],
      positionSummary: "Rather than worshipping Set in a posture of submission, Setians describe themselves as students or companions of Set, pursuing their own godlike self-aware intelligence as an end in itself.",
      verses: [
        { ref: "Psalm 95:6", text: "O come, let us worship and bow down: let us kneel before the LORD our maker." },
        { ref: "Exodus 20:3", text: "Thou shalt have no other gods before me." }
      ]
    },
    {
      claim: "Satan is worshipped and petitioned as a literal being through ceremonial ritual magic",
      religion: "Theistic Satanism",
      keywords: ["theistic satanism literal satan worship", "ceremonial magic satan worship", "devil worship ritual"],
      positionSummary: "Unlike LaVeyan Satanism's atheistic, symbolic framework, theistic Satanists — a loose, unorganized category rather than one movement, spanning many independent and sometimes rival individuals and small groups — hold that Satan is a real supernatural being to be venerated, invoked, or petitioned through ceremonial magic ritual.",
      verses: [
        { ref: "Deuteronomy 18:10-12", text: "There shall not be found among you any one that maketh his son or his daughter to pass through the fire, or that useth divination, or an observer of times, or an enchanter, or a witch... For all that do these things are an abomination unto the LORD." },
        { ref: "Isaiah 8:19", text: "And when they shall say unto you, Seek unto them that have familiar spirits, and unto wizards that peep, and that mutter: should not a people seek unto their God?" }
      ]
    },
    {
      claim: "Ritual invocation and evocation are used to commune with or draw on the power of Satan and other spirits",
      religion: "Theistic Satanism",
      keywords: ["theistic satanism invocation evocation", "ritual magic demons theistic satanism"],
      positionSummary: "Practices across theistic Satanist groups commonly include ceremonial invocation (inviting a spirit's presence or influence into the practitioner) and evocation (summoning a spirit to manifest and be commanded or petitioned) — techniques adapted from older ceremonial magic traditions.",
      verses: [
        { ref: "Leviticus 19:31", text: "Regard not them that have familiar spirits, neither seek after wizards, to be defiled by them: I am the LORD your God." },
        { ref: "Ephesians 6:12", text: "For we wrestle not against flesh and blood, but against principalities, against powers, against the rulers of the darkness of this world, against spiritual wickedness in high places." }
      ]
    },
    {
      claim: "The Order of Nine Angles (O9A) is an occult neo-Nazi network that U.S. federal prosecutors have called \"a racially motivated violent extremist group\"",
      religion: "Order of Nine Angles",
      keywords: ["order of nine angles o9a", "occult neo-nazi extremist group", "o9a federal prosecutors"],
      positionSummary: "This entry exists only to document and warn against O9A, not to present its ideology as a legitimate alternative belief system — no O9A material is quoted or summarized here. U.S. federal prosecutors have officially described the group as \"an occult-based neo-Nazi and racially motivated violent extremist group.\" A joint threat assessment by the National Counterterrorism Center, FBI, and Department of Homeland Security identified it as a growing influence among white supremacists, and UK anti-extremism group Hope Not Hate has called for it to be formally proscribed as a terrorist organization.",
      verses: [
        { ref: "1 John 4:1", text: "Beloved, believe not every spirit, but try the spirits whether they are of God: because many false prophets are gone out into the world." },
        { ref: "Ephesians 6:12", text: "For we wrestle not against flesh and blood, but against principalities, against powers, against the rulers of the darkness of this world, against spiritual wickedness in high places." }
      ]
    },
    {
      claim: "O9A ideology has been directly linked to real terrorist violence, including the 1999 London nail bombings and multiple UK terrorism convictions",
      religion: "Order of Nine Angles",
      keywords: ["order of nine angles terrorism", "david copeland nail bombings o9a", "o9a terrorism convictions"],
      positionSummary: "David Copeland, who carried out the 1999 London nail bombings targeting Black, Bangladeshi, and gay communities — killing 3 people and injuring over 100 — was a neo-Nazi militant reported to have been influenced by O9A-linked material. Hope Not Hate documented at least eight UK terrorism convictions between 2019 and 2021 tied to O9A influence.",
      verses: [
        { ref: "John 8:44", text: "Ye are of your father the devil, and the lusts of your father ye will do. He was a murderer from the beginning, and abode not in the truth, because there is no truth in him." },
        { ref: "Galatians 5:19-21", text: "Now the works of the flesh are manifest... hatred, variance, emulations, wrath, strife, seditions... murders... they which do such things shall not inherit the kingdom of God." }
      ]
    },
    {
      claim: "The supreme creator is too distant to petition directly; practitioners instead venerate and work through intermediary Orishas, syncretized with Catholic saints",
      religion: "Santeria",
      keywords: ["santeria orishas intermediaries", "olodumare distant creator", "orishas syncretized catholic saints"],
      positionSummary: "In Santería (Regla de Ocha, an Afro-Cuban tradition blending Yoruba Orisha veneration with Catholicism), the supreme being Olodumare/Olorun is held to be too remote for direct petition. Devotion instead centers on Orishas — Elegua, Ogun, Yemayá, Changó, Obatalá, Oshún, and others — each paired with a Catholic saint (Changó with Saint Barbara, Yemayá with Our Lady of Regla, Elegua with Saint Anthony) as intermediaries who are petitioned for specific help and protection.",
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Animal sacrifice is offered to nourish the Orishas and secure their favor and power",
      religion: "Santeria",
      keywords: ["santeria animal sacrifice", "ashe orisha offerings", "santeria ritual sacrifice"],
      positionSummary: "Animal sacrifice — offering blood and life-force (ashé) to the Orishas — is a central, legally-recognized ritual practice in Santería (affirmed as protected religious exercise by the U.S. Supreme Court in Church of the Lukumi Babalu Aye v. City of Hialeah, 1993), understood as feeding and empowering the Orishas in exchange for their continued blessing and protection.",
      verses: [
        { ref: "Hebrews 10:10-12", text: "By the which will we are sanctified through the offering of the body of Jesus Christ once for all... But this man, after he had offered one sacrifice for sins for ever, sat down on the right hand of God;" },
        { ref: "Psalm 50:12-13", text: "If I were hungry, I would not tell thee: for the world is mine, and the fulness thereof. Will I eat the flesh of bulls, or drink the blood of goats?" }
      ]
    },
    {
      claim: "Divination through cowrie shells or a babalawo priest reveals one's destiny and the Orishas' guidance",
      religion: "Santeria",
      keywords: ["santeria divination diloggun", "babalawo ifa divination", "santeria cowrie shells"],
      positionSummary: "Practitioners consult the Orishas' will through divination systems such as the diloggún (cowrie shells) or, for deeper questions, through a babalawo priest using the Ifá system — seeking to learn one's destiny and receive guidance directly from the spirit world.",
      verses: [
        { ref: "Deuteronomy 18:10-12", text: "There shall not be found among you any one that maketh his son or his daughter to pass through the fire, or that useth divination, or an observer of times... For all that do these things are an abomination unto the LORD." },
        { ref: "Isaiah 8:19", text: "And when they shall say unto you, Seek unto them that have familiar spirits, and unto wizards that peep, and that mutter: should not a people seek unto their God?" }
      ]
    },
    {
      claim: "During ceremonies, an Orisha is believed to possess and speak or act through an initiate, who becomes its \"horse\"",
      religion: "Santeria",
      keywords: ["santeria spirit possession", "orisha mounting horse caballo", "santeria trance ceremony"],
      positionSummary: "In Santería ceremony, an initiate (\"caballo\", or horse) may become possessed or \"mounted\" by an Orisha, who is understood to temporarily take control of the person's body and consciousness to speak, dance, or act directly among the community.",
      verses: [
        { ref: "1 Corinthians 14:32", text: "And the spirits of the prophets are subject to the prophets." },
        { ref: "2 Timothy 1:7", text: "For God hath not given us the spirit of fear; but of power, and of love, and of a sound mind." }
      ]
    },
    {
      claim: "Olorun, the supreme sky-god, is considered too distant or indifferent to interfere in the affairs of the world",
      religion: "Yoruba Religion",
      keywords: ["olorun distant sky god", "yoruba supreme god uninvolved", "olorun idleness repose"],
      sourceQuote: { ref: "Yoruba Religion (Ellis, 1894), Ch. II", text: "Olorun is considered too distant, or too indifferent, to interfere in the affairs of the world. The natives say that he enjoys a life of complete idleness and repose, a blissful condition according to their ideas." },
      verses: [
        { ref: "Acts 17:27", text: "That they should seek the Lord, if haply they might feel after him, and find him, though he be not far from every one of us:" },
        { ref: "Psalm 145:18", text: "The LORD is nigh unto all them that call upon him, to all that call upon him in truth." }
      ]
    },
    {
      claim: "A babalawo priest divines the will of the gods by casting sixteen palm-nuts; nothing important is undertaken without consulting him",
      religion: "Yoruba Religion",
      keywords: ["babalawo ifa palm nuts divination", "yoruba ifa priest consultation", "ifa god of divination"],
      sourceQuote: { ref: "Yoruba Religion (Ellis, 1894), Ch. II & V", text: "A priest of Ifa is termed a babalawo... and the profession is very lucrative, as the natives never undertake anything of importance without consulting the god, and always act in accordance with the answer returned... it is through his agency, as the priest of Ifa, the god of divination, that man learns what is necessary to be done to please the other gods." },
      verses: [
        { ref: "Deuteronomy 18:10-12", text: "There shall not be found among you any one that maketh his son or his daughter to pass through the fire, or that useth divination... For all that do these things are an abomination unto the LORD." },
        { ref: "James 1:5", text: "If any of you lack wisdom, let him ask of God, that giveth to all men liberally, and upbraideth not; and it shall be given him." }
      ]
    },
    {
      claim: "A masked man representing an ancestor risen from the dead (Egungun) appears among the living to bring news from the land of the dead",
      religion: "Yoruba Religion",
      keywords: ["egungun ancestor risen from dead", "yoruba masked spirit ancestor", "egungun bone skeleton"],
      sourceQuote: { ref: "Yoruba Religion (Ellis, 1894), Ch. VI", text: "Egungun himself is supposed to be a man risen from the dead... He is supposed to have returned from the land of the dead in order to ascertain what is going on in the land of the living." },
      verses: [
        { ref: "Luke 16:31", text: "And he said unto him, If they hear not Moses and the prophets, neither will they be persuaded, though one rose from the dead." },
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" }
      ]
    },
    {
      claim: "Each person is inhabited by multiple distinct spirits located in different parts of the body, each receiving its own offerings",
      religion: "Yoruba Religion",
      keywords: ["yoruba indwelling spirits body", "olori ipin ipori", "multiple souls yoruba religion"],
      sourceQuote: { ref: "Yoruba Religion (Ellis, 1894), Ch. VII", text: "Olori, dwells in the head, the second, Ipin ijeun, in the stomach, and the third, Ipori, in the great toe... Offerings are made to him, chiefly fowls." },
      note: "Three separate indwelling spirits are described, each located in a different part of the body and each the recipient of its own distinct sacrifice.",
      verses: [
        { ref: "1 Thessalonians 5:23", text: "And the very God of peace sanctify you wholly; and I pray God your whole spirit and soul and body be preserved blameless unto the coming of our Lord Jesus Christ." }
      ]
    },
    {
      claim: "Papa Legba, guardian of the gates, must be petitioned first in every ceremony to open the way to the other spirits",
      religion: "Vodou",
      keywords: ["papa legba gatekeeper vodou", "legba open the gate", "vodou crossroads spirit"],
      sourceQuote: { ref: "The Magic Island (Seabrook, 1929), Ch. V", text: "Papa Legba, ouvri barrière pour li passer! ... (Father Legba, open wide the gate that he may pass!)" },
      note: "According to this account — a 1929 outsider's memoir, not a verified or authoritative Vodou source — Legba is invoked at the start of ceremony as the gatekeeper who alone can open the way to every other spirit.",
      verses: [
        { ref: "John 10:9", text: "I am the door: by me if any man enter in, he shall be saved, and shall go in and out, and find pasture." },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Vodou venerates a large pantheon of loa — spirits such as Damballa, Agoué, and Ezilée — each paired with a Catholic saint and each governing a different domain of life",
      religion: "Vodou",
      keywords: ["vodou loa pantheon", "damballa agoue ezilee", "vodou catholic saint syncretism"],
      sourceQuote: { ref: "The Magic Island (Seabrook, 1929), Ch. I", text: "Papa Legba, guardian of the gates, who was the most benevolent; Damballa Oueddo, wisest and most powerful, whose symbol was the serpent; Loco, god of the forests; Agoué, god of the sea; Maitresse Ezilée, who was the mild Blessed Virgin Mary; Ogoun Badagris, the bloody dreadful One whose voice was thunder." },
      note: "According to this account — a 1929 outsider's memoir, not a verified or authoritative Vodou source — each loa was described to the author as syncretized with a specific Catholic saint.",
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "Exodus 20:3", text: "Thou shalt have no other gods before me." }
      ]
    },
    {
      claim: "Animal sacrifice — the blood poured out to the loa — is the central offering of Vodou ceremony",
      religion: "Vodou",
      keywords: ["vodou animal sacrifice", "petro rite blood offering", "vodou ceremonial sacrifice"],
      sourceQuote: { ref: "The Magic Island (Seabrook, 1929), Ch. III", text: "The papaloi plunged the long, pointed blade beneath the bull's shoulder and through its heart... it spurted in a hard, small stream from the bull's pierced side, where the mamaloi knelt with her bowl to receive it and transferred it bowl by bowl to the great common trough." },
      note: "According to this account — a 1929 outsider's memoir, not a verified or authoritative Vodou source — of a Petro rite the author witnessed and personally took part in.",
      verses: [
        { ref: "Hebrews 10:10-12", text: "By the which will we are sanctified through the offering of the body of Jesus Christ once for all... But this man, after he had offered one sacrifice for sins for ever, sat down on the right hand of God;" },
        { ref: "Psalm 50:12-13", text: "If I were hungry, I would not tell thee: for the world is mine, and the fulness thereof. Will I eat the flesh of bulls, or drink the blood of goats?" }
      ]
    },
    {
      claim: "During ceremony, a loa is believed to descend and take possession of a worshipper's body, speaking and acting through them until it departs",
      religion: "Vodou",
      keywords: ["vodou spirit possession loa", "mounted by a loa", "vodou trance ceremony"],
      sourceQuote: { ref: "The Magic Island (Seabrook, 1929), Ch. VI", text: "The god, replete with food and quietly drunken, lay down to sleep alone in his silent temple. But when morning came, the god had departed. Only an humble ragged negro lay there dozing at the foot of the altar." },
      note: "According to this account — a 1929 outsider's memoir, not a verified or authoritative Vodou source — of a man reportedly possessed by the loa Ogoun Badagris for the course of an evening's ceremony.",
      verses: [
        { ref: "1 Corinthians 14:32", text: "And the spirits of the prophets are subject to the prophets." },
        { ref: "2 Timothy 1:7", text: "For God hath not given us the spirit of fear; but of power, and of love, and of a sound mind." }
      ]
    },
    {
      claim: "An unseen power flows down from the heavens and pervades the whole earth, physically altering the elements, plants, and animals according to the motions of the stars",
      religion: "Astrology",
      keywords: ["tetrabiblos aethereal power pervades earth", "ptolemy celestial power nature", "astrology stars physical influence"],
      sourceQuote: { ref: "Ptolemy's Tetrabiblos (Ashmand, 1822), Book I, Ch. 2, para. 1", text: "That a certain power, derived from the æthereal nature, is diffused over and pervades the whole atmosphere of the earth, is clearly evident to all men. Fire and air, the first of the sublunary elements, are encompassed and altered by the motions of the æther. These elements in their turn encompass all inferior matter, and vary it as they themselves are varied; acting on earth and water, on plants and animals." },
      note: "Opens the second chapter of Ptolemy's Tetrabiblos ('Knowledge May Be Acquired by Astronomy to a Certain Extent') — the founding claim of the entire text: the heavens act as a real physical cause reaching down into earthly life.",
      verses: [
        { ref: "Jeremiah 10:2", text: "Thus saith the LORD, Learn not the way of the heathen, and be not dismayed at the signs of heaven; for the heathen are dismayed at them." },
        { ref: "Deuteronomy 4:19", text: "And lest thou lift up thine eyes unto heaven, and when thou seest the sun, and the moon, and the stars, even all the host of heaven, shouldest be driven to worship them, and serve them, which the LORD thy God hath divided unto all nations under the whole heaven." }
      ]
    },
    {
      claim: "The Sun, Moon, and planets are physical causes that 'dispose' the character and constitution of everything beneath them, each according to its own inherent nature",
      religion: "Astrology",
      keywords: ["tetrabiblos influences of planetary orbs", "sun disposes inhabitants nature", "astrology planets cause character"],
      sourceQuote: { ref: "Ptolemy's Tetrabiblos (Ashmand, 1822), Book I, Ch. 4, para. 1", text: "The Sun is found to produce heat and moderate dryness. His magnitude, and the changes which he so evidently makes in the seasons, render his power more plainly perceptible than that of the other heavenly bodies; since his approach to the zenith of any part of the earth creates a greater degree of heat in that part and proportionately disposes its inhabitants after his own nature." },
      note: "From the chapter 'The Influences of the Planetary Orbs,' which assigns each planet its own causal power (heat, cold, moisture, dryness) that shapes the nature of whatever falls under it.",
      verses: [
        { ref: "Job 38:31", text: "Canst thou bind the sweet influences of Pleiades, or loose the bands of Orion?" },
        { ref: "Job 38:33", text: "Knowest thou the ordinances of heaven? canst thou set the dominion thereof in the earth?" }
      ]
    },
    {
      claim: "Eclipses of the Sun and Moon are 'the strongest and principal cause' determining the fate of cities, countries, and kings",
      religion: "Astrology",
      keywords: ["tetrabiblos eclipses cause nations", "astrology eclipse predicts kings", "ptolemy conjunctions cities countries"],
      sourceQuote: { ref: "Ptolemy's Tetrabiblos (Ashmand, 1822), Book II, Ch. 5, para. 2", text: "The strongest and principal cause of all these events exists in the ecliptical conjunctions of the Sun and Moon, and in the several transits made by the planets during those conjunctions." },
      note: "Opens Ptolemy's method for 'mundane' astrology — predicting the fortunes of nations and rulers, rather than individuals, from the location and timing of eclipses.",
      verses: [
        { ref: "Isaiah 47:13-14", text: "Thou art wearied in the multitude of thy counsels. Let now the astrologers, the stargazers, the monthly prognosticators, stand up, and save thee from these things that shall come upon thee. Behold, they shall be as stubble; the fire shall burn them; they shall not deliver themselves from the power of the flame." }
      ]
    },
    {
      claim: "The length of a person's life is the most essential thing to determine from a birth chart, and can be computed in advance from the horoscope's 'prorogatory places'",
      religion: "Astrology",
      keywords: ["tetrabiblos duration of life", "astrology predicts lifespan birth chart", "ptolemy prorogators death"],
      sourceQuote: { ref: "Ptolemy's Tetrabiblos (Ashmand, 1822), Book III, Ch. 11, para. 1", text: "Of all events whatsoever, which take place after birth, the most essential is the continuance of life... the inquiry into the duration of life consequently takes precedence of all other questions, as to the events subsequent to the birth." },
      verses: [
        { ref: "Psalm 139:16", text: "Thine eyes did see my substance, yet being unperfect; and in thy book all my members were written, which in continuance were fashioned, when as yet there was none of them." },
        { ref: "Job 14:5", text: "Seeing his days are determined, the number of his months are with thee, thou hast appointed his bounds that he cannot pass;" }
      ]
    },
    {
      claim: "The exact manner and cause of a person's death — down to the specific diseases — is fixed in advance by whichever planet holds 'dominion' over it at birth",
      religion: "Astrology",
      keywords: ["tetrabiblos kind of death planet", "saturn diseases death astrology", "ptolemy dominion of death"],
      sourceQuote: { ref: "Ptolemy's Tetrabiblos (Ashmand, 1822), Book IV, Ch. 9, para. 2", text: "Thus, for example, if the dominion of death be vested in Saturn, he will produce death by means of lingering diseases; cough, rheumatism, flux, ague, disorder of the spleen, dropsy, colic, and complaints in the womb; and, in short, by all such diseases as proceed from the superabundance of cold." },
      verses: [
        { ref: "James 4:13-15", text: "Go to now, ye that say, To day or to morrow we will go into such a city... whereas ye know not what shall be on the morrow. For what is your life? It is even a vapour, that appeareth for a little time, and then vanisheth away. For that ye ought to say, If the Lord will, we shall live, and do this, or that." }
      ]
    },
    {
      claim: "Whether, when, and how many times a man will marry — and even his future wife's temperament — is fixed in advance by the Moon's position and aspects at his birth",
      religion: "Astrology",
      keywords: ["tetrabiblos marriage moon position", "astrology predicts wife character", "ptolemy nativities marriage"],
      sourceQuote: { ref: "Ptolemy's Tetrabiblos (Ashmand, 1822), Book IV, Ch. 5, para. 2", text: "With regard to men, it is to be observed in what manner the Moon may be disposed; for, in the first place, if she be found in the oriental quadrants, she will cause men either to marry early in life, or, after having over-passed their prime, to marry young women... and if she be found under the Sun's beams, and configurated with Saturn, she then entirely denies marriage." },
      verses: [
        { ref: "Proverbs 19:21", text: "There are many devices in a man's heart; nevertheless the counsel of the LORD, that shall stand." }
      ]
    },
    {
      claim: "Souls are purified in Purgatory after death before entering heaven",
      religion: "Catholicism",
      keywords: ["purgatory", "catholic purgatory doctrine", "temporal punishment after death", "purified after death"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 414", text: "Purgatory is the state in which those suffer for a time who die guilty of venial sins, or without having satisfied for the punishment due to their sins." },
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" },
        { ref: "Luke 23:43", text: "And Jesus said unto him, Verily I say unto thee, To day shalt thou be with me in paradise." },
        { ref: "2 Corinthians 5:8", text: "We are confident, I say, and willing rather to be absent from the body, and to be present with the Lord." }
      ]
    },
    {
      claim: "Mary was conceived without original sin (the Immaculate Conception)",
      religion: "Catholicism",
      keywords: ["immaculate conception", "mary sinless", "mary without original sin", "mary full of grace"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 50", text: "The Blessed Virgin Mary, through the merits of her divine Son, was preserved free from the guilt of Original Sin, and this privilege is called her Immaculate Conception." },
      note: "Defined as dogma by Pope Pius IX in the bull Ineffabilis Deus (1854).",
      verses: [
        { ref: "Romans 3:23", text: "For all have sinned, and come short of the glory of God." },
        { ref: "Romans 5:12", text: "Wherefore, as by one man sin entered into the world, and death by sin; and so death passed upon all men, for that all have sinned:" },
        { ref: "Luke 1:47", text: "And my spirit hath rejoiced in God my Saviour." }
      ]
    },
    {
      claim: "There is no salvation for anyone who knowingly remains outside the Catholic Church",
      religion: "Catholicism",
      keywords: ["no salvation outside the church", "extra ecclesiam nulla salus", "catholic church necessary for salvation"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 121", text: "All are bound to belong to the Church, and he who knows the Church to be the true Church and remains out of it, cannot be saved." },
      verses: [
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." },
        { ref: "Romans 10:9", text: "That if thou shalt confess with thy mouth the Lord Jesus, and shalt believe in thine heart that God hath raised him from the dead, thou shalt be saved." },
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." }
      ]
    },
    {
      claim: "The Pope, as successor of Peter, can teach infallibly on faith and morals",
      religion: "Catholicism",
      keywords: ["papal infallibility", "pope supreme authority", "petrine primacy", "vicar of christ"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 125", text: "The Church teaches infallibly when it speaks through the Pope and bishops united in general council, or through the Pope alone when he proclaims to all the faithful a doctrine of faith or morals." },
      note: "Papal infallibility exercised apart from a council was formally defined by the First Vatican Council in Pastor Aeternus (1870).",
      verses: [
        { ref: "Galatians 2:11", text: "But when Peter was come to Antioch, I withstood him to the face, because he was to be blamed." },
        { ref: "Acts 15:6-7", text: "And the apostles and elders came together for to consider of this matter. And when there had been much disputing, Peter rose up, and said unto them, Men and brethren, ye know how that a good while ago God made choice among us..." },
        { ref: "1 Peter 5:1", text: "The elders which are among you I exhort, who am also an elder, and a witness of the sufferings of Christ, and also a partaker of the glory that shall be revealed:" }
      ]
    },
    {
      claim: "Baptism is necessary for salvation",
      religion: "Catholicism",
      keywords: ["baptism necessary for salvation", "baptismal regeneration catholic", "baptism washes away sin"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 154", text: "Baptism is necessary to salvation, because without it we cannot enter into the kingdom of Heaven." },
      verses: [
        { ref: "Luke 23:43", text: "And Jesus said unto him, Verily I say unto thee, To day shalt thou be with me in paradise." },
        { ref: "Romans 10:9", text: "That if thou shalt confess with thy mouth the Lord Jesus, and shalt believe in thine heart that God hath raised him from the dead, thou shalt be saved." },
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." }
      ]
    },
    {
      claim: "The bread and wine of the Eucharist become the actual body and blood of Christ",
      religion: "Catholicism",
      keywords: ["transubstantiation", "real presence eucharist", "catholic communion body of christ", "eucharist literal body and blood"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 244, 246", text: "After the substance of the bread and wine had been changed into the substance of the body and blood of Our Lord there remained only the appearances of bread and wine. This change of the bread and wine into the body and blood of Our Lord is called Transubstantiation." },
      verses: [
        { ref: "Luke 22:19", text: "And he took bread, and gave thanks, and brake it, and gave unto them, saying, This is my body which is given for you: this do in remembrance of me." },
        { ref: "John 6:63", text: "It is the spirit that quickeneth; the flesh profiteth nothing: the words that I speak unto you, they are spirit, and they are life." }
      ]
    },
    {
      claim: "Mortal sins must be confessed to a priest to be absolved",
      religion: "Catholicism",
      keywords: ["confession to a priest", "catholic confession sin", "sacrament of penance", "auricular confession"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 209", text: "We are bound to confess all our mortal sins, but it is well also to confess our venial sins." },
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "1 John 1:9", text: "If we confess our sins, he is faithful and just to forgive us our sins, and to cleanse us from all unrighteousness." }
      ]
    },
    {
      claim: "The saints can be invoked in prayer to ask for their help and intercession",
      religion: "Catholicism",
      keywords: ["intercession of saints", "praying to saints", "mary mediatrix", "invocation of saints catholic"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 332, 333", text: "The First Commandment does not forbid us to pray to the saints. By praying to the saints we mean the asking of their help and prayers." },
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "Hebrews 4:16", text: "Let us therefore come boldly unto the throne of grace, that we may obtain mercy, and find grace to help in time of need." }
      ]
    },
    {
      claim: "Grace is necessary to merit Heaven — good works done in grace merit salvation",
      religion: "Catholicism",
      keywords: ["merit catholic salvation", "faith and works catholic", "cooperate with grace", "earn salvation catholic"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 111", text: "Grace is necessary for salvation, because without grace we can do nothing to merit Heaven." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" }
      ]
    },
    {
      claim: "The Church can grant indulgences that remit the temporal punishment due for sin",
      religion: "Catholicism",
      keywords: ["indulgences catholic", "remission of temporal punishment", "catholic indulgence doctrine"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 231", text: "An indulgence is the remission in whole or in part of the temporal punishment due to sin." },
      verses: [
        { ref: "Ephesians 1:7", text: "In whom we have redemption through his blood, the forgiveness of sins, according to the riches of his grace;" },
        { ref: "Colossians 2:13-14", text: "Having forgiven you all trespasses; Blotting out the handwriting of ordinances that was against us, which was contrary to us, and took it out of the way, nailing it to his cross;" }
      ]
    },
    {
      claim: "Justification by faith alone, with nothing else required, is formally condemned as anathema",
      religion: "Catholicism",
      keywords: ["sola fide anathema", "faith alone condemned catholic", "council of trent justification", "trent canon on faith alone"],
      sourceQuote: { ref: "Council of Trent, Session the Sixth (On Justification), Canon IX", text: "If any one saith, that by faith alone the impious is justified; in such wise as to mean, that nothing else is required to co-operate in order to the obtaining the grace of Justification, and that it is not in any way necessary, that he be prepared and disposed by the movement of his own will; let him be anathema." },
      note: "This is Trent's direct, formal condemnation of sola fide, the doctrine that justification is by faith alone.",
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Romans 3:28", text: "Therefore we conclude that a man is justified by faith without the deeds of the law." },
        { ref: "Galatians 2:16", text: "Knowing that a man is not justified by the works of the law, but by the faith of Jesus Christ, even we have believed in Jesus Christ, that we might be justified by the faith of Christ, and not by the works of the law: for by the works of the law shall no flesh be justified." }
      ]
    },
    /*
     * These two quote the Second Vatican Council directly (Lumen Gentium,
     * 1964; Nostra Aetate, 1965), not the Baltimore Catechism above. Unlike
     * that text, no confirmed-public-domain English translation of either
     * document exists — every edition in circulation is claimed under
     * copyright by some party (Libreria Editrice Vaticana, or Sheed & Ward /
     * Trustees for Roman Catholic Purposes for the Flannery edition). So
     * these are kept to a single short, attributed excerpt each rather than
     * a bulk corpus like the other sources in this file.
     */
    {
      claim: "Those who through no fault of their own never come to know Christ or the Church can still attain salvation",
      religion: "Catholicism",
      keywords: ["invincible ignorance salvation", "lumen gentium salvation", "saved without knowing christ catholic", "sincere seekers of god saved"],
      sourceQuote: { ref: "Lumen Gentium §16 (Second Vatican Council, 1964)", text: "Those also can attain to everlasting salvation who through no fault of their own do not know the Gospel of Christ or His Church, yet sincerely seek God and, moved by grace, try in their actions to do His will as they know it through the dictates of their conscience." }, // VERIFY WORDING — translation edition
      verses: [
        { ref: "Romans 10:14", text: "How then shall they call on him in whom they have not believed? and how shall they believe in him of whom they have not heard? and how shall they hear without a preacher?" },
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Non-Christian religions can reflect a genuine ray of the truth that enlightens all people",
      religion: "Catholicism",
      keywords: ["nostra aetate other religions", "rays of truth other religions catholic", "catholic view of world religions", "vatican ii non-christian religions"],
      sourceQuote: { ref: "Nostra Aetate §2 (Second Vatican Council, 1965)", text: "The Catholic Church rejects nothing that is true and holy in these religions... these often reflect a ray of that Truth which enlightens all men." }, // VERIFY WORDING — translation edition
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." },
        { ref: "2 Corinthians 6:14", text: "Be ye not unequally yoked together with unbelievers: for what fellowship hath righteousness with unrighteousness? and what communion hath light with darkness?" }
      ]
    },
    /*
     * These three fill gaps spotted by cross-checking against lotwi.org's
     * "Catholicism" objections page (Light of the World Initiative) — used
     * only to find topics not yet covered here, not as a text source: its
     * own written responses are that ministry's original copyrighted
     * commentary, not reused below. Each entry here is independently
     * sourced against this app's own verified Baltimore Catechism data,
     * or (where no pre-1929 public-domain primary source exists for a
     * dogma) written as a plain positionSummary instead of inventing a
     * quote.
     */
    {
      claim: "The Pope is Peter's successor and, as visible head of the Church, holds supreme authority over it",
      religion: "Catholicism",
      keywords: ["pope successor of peter", "petrine succession", "vicar of christ visible head", "papal supreme authority"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 117, 118", text: "Our Holy Father the Pope, the Bishop of Rome, is the vicar of Christ on earth and the visible head of the Church... because he is the successor of St. Peter, whom Christ made the chief of the Apostles and the visible head of the Church." },
      note: "Distinct from papal infallibility (a specific teaching mechanism) — this is the broader claim of Peter's succession and headship itself.",
      verses: [
        { ref: "Colossians 1:18", text: "And he is the head of the body, the church: who is the beginning, the firstborn from the dead; that in all things he might have the preeminence." },
        { ref: "Matthew 23:9", text: "And call no man your father upon the earth: for one is your Father, which is in heaven." },
        { ref: "Ephesians 1:22", text: "And hath put all things under his feet, and gave him to be the head over all things to the church," }
      ]
    },
    {
      claim: "Mary was bodily assumed into heaven at the end of her earthly life",
      religion: "Catholicism",
      keywords: ["assumption of mary", "mary taken up to heaven", "bodily assumption catholic"],
      positionSummary: "Defined as dogma by Pope Pius XII in Munificentissimus Deus (1950): that Mary, having completed her earthly life, was assumed body and soul into heavenly glory. No pre-1929 public-domain primary source exists for this one — it was formally defined after that cutoff — so it's stated here rather than quoted.",
      verses: [
        { ref: "John 3:13", text: "And no man hath ascended up to heaven, but he that came down from heaven, even the Son of man which is in heaven." },
        { ref: "Acts 13:36", text: "For David, after he had served his own generation by the will of God, fell on sleep, and was laid unto his fathers, and saw corruption:" }
      ]
    },
    {
      claim: "Priests must remain celibate and unmarried to serve God more fully",
      religion: "Catholicism",
      keywords: ["priestly celibacy", "catholic priests cannot marry", "clerical celibacy discipline"],
      positionSummary: "Latin Rite discipline (not dogma) requires priests to remain unmarried, understood as freeing them for undivided devotion to ministry.",
      verses: [
        { ref: "1 Timothy 4:1-3", text: "Now the Spirit speaketh expressly, that in the latter times some shall depart from the faith, giving heed to seducing spirits, and doctrines of devils; Speaking lies in hypocrisy; having their conscience seared with a hot iron; Forbidding to marry, and commanding to abstain from meats, which God hath created to be received with thanksgiving of them which believe and know the truth." },
        { ref: "1 Corinthians 9:5", text: "Have we not power to lead about a sister, a wife, as well as other apostles, and as the brethren of the Lord, and Cephas?" },
        { ref: "1 Timothy 3:2", text: "A bishop then must be blameless, the husband of one wife, vigilant, sober, of good behaviour, given to hospitality, apt to teach;" }
      ]
    },
    {
      claim: "Sacred images and statues may be venerated because they are reminders of Christ and the saints, not gods themselves",
      religion: "Catholicism",
      keywords: ["veneration of images catholic", "statues not idols", "catholic images second commandment", "praying before images"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 341", text: "The First Commandment does forbid the making of images if they are made to be adored as gods, but it does not forbid the making of them to put us in mind of Jesus Christ, His Blessed Mother, and the saints." },
      note: "Q. 343 of the same catechism is explicit that Catholics are taught not to pray to the images themselves: \"It is not allowed to pray to the crucifix or images and relics of the saints, for they have no life, nor power to help us, nor sense to hear us.\"",
      verses: [
        { ref: "Exodus 20:4-5", text: "Thou shalt not make unto thee any graven image, or any likeness of any thing that is in heaven above, or that is in the earth beneath, or that is in the water under the earth: Thou shalt not bow down thyself to them, nor serve them..." },
        { ref: "Isaiah 44:9", text: "They that make a graven image are all of them vanity; and their delectable things shall not profit..." }
      ]
    },
    {
      claim: "The Mass is a true, propitiatory sacrifice offered for the living and the dead, not merely a commemoration of Calvary",
      religion: "Catholicism",
      keywords: ["sacrifice of the mass", "mass propitiatory sacrifice", "trent mass canon", "eucharist re-presents calvary"],
      sourceQuote: { ref: "Council of Trent, Session the Twenty-Second (On The Sacrifice Of The Mass), Canon III", text: "If any one saith, that the sacrifice of the mass is only a sacrifice of praise and of thanksgiving; or, that it is a bare commemoration of the sacrifice consummated on the cross, but not a propitiatory sacrifice; or, that it profits him only who receives; and that it ought not to be offered for the living and the dead for sins, pains, satisfactions, and other necessities; let him be anathema." },
      verses: [
        { ref: "Hebrews 9:25-28", text: "Nor yet that he should offer himself often... but now once in the end of the world hath he appeared to put away sin by the sacrifice of himself... so Christ was once offered to bear the sins of many..." },
        { ref: "Hebrews 10:12-14", text: "But this man, after he had offered one sacrifice for sins for ever, sat down on the right hand of God... For by one offering he hath perfected for ever them that are sanctified." }
      ]
    },
    /*
     * These three are tagged "Apocrypha", not "Catholicism" — they're
     * specific claims found within the deuterocanonical books themselves
     * (see the searchable Apocrypha/Deuterocanon source text above), which
     * is historically why the Reformers rejected these books' canonicity:
     * not on manuscript grounds alone, but because passages like these
     * conflict with sola fide/sola gratia. Framed as claims from the text
     * itself, not as claims about what Catholics believe.
     */
    {
      claim: "Prayer for the dead is holy and wholesome, and can free them from their sins",
      religion: "Apocrypha",
      keywords: ["prayer for the dead", "pray for the dead purgatory", "2 maccabees prayer for dead", "loosed from sins after death"],
      sourceQuote: { ref: "2 Machabees 12:46", text: "It is therefore a holy and wholesome thought to pray for the dead, that they may be loosed from sins." },
      note: "This passage, describing Judas Machabeus taking up a collection for a sin offering on behalf of fallen soldiers, is the primary text cited in support of prayer for the dead and Purgatory.",
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" },
        { ref: "Luke 16:26", text: "And beside all this, between us and you there is a great gulf fixed: so that they which would pass from hence to you cannot; neither can they pass to us, that would come from thence." },
        { ref: "2 Corinthians 5:10", text: "For we must all appear before the judgment seat of Christ; that every one may receive the things done in his body, according to that he hath done, whether it be good or bad." }
      ]
    },
    {
      claim: "Almsgiving delivers from death and purges away sin",
      religion: "Apocrypha",
      keywords: ["alms purge sin", "almsgiving delivers from death", "tobit alms", "works atone for sin"],
      sourceQuote: { ref: "Tobias 12:9", text: "For alms delivereth from death, and the same is that which purgeth away sins, and maketh to find mercy and life everlasting." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Isaiah 64:6", text: "But we are all as an unclean thing, and all our righteousnesses are as filthy rags; and we all do fade as a leaf; and our iniquities, like the wind, have taken us away." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" }
      ]
    },
    {
      claim: "God left man entirely in the hand of his own free counsel to choose good or evil",
      religion: "Apocrypha",
      keywords: ["free will apocrypha", "sirach free will", "hand of his own counsel", "stretch forth thy hand to which thou wilt"],
      sourceQuote: { ref: "Ecclesiasticus 15:14", text: "God made man from the beginning, and left him in the hand of his own counsel." },
      note: "The passage continues in 15:17: \"He hath set water and fire before thee: stretch forth thy hand to which thou wilt.\"",
      verses: [
        { ref: "Ephesians 2:1", text: "And you hath he quickened, who were dead in trespasses and sins;" },
        { ref: "John 15:16", text: "Ye have not chosen me, but I have chosen you, and ordained you, that ye should go and bring forth fruit, and that your fruit should remain: that whatsoever ye shall ask of the Father in my name, he may give it you." },
        { ref: "Romans 9:16", text: "So then it is not of him that willeth, nor of him that runneth, but of God that sheweth mercy." }
      ]
    },
    {
      claim: "The deuterocanonical books belong in the Bible as inspired Scripture",
      religion: "Apocrypha",
      keywords: ["apocrypha canonicity", "deuterocanon inspired scripture", "apocrypha belongs in the bible", "canon of scripture apocrypha"],
      positionSummary: "The Council of Trent (1546) formally defined these books as canonical Scripture, against the Reformers, who excluded them, following the Jewish canon — the Hebrew Bible never included them, and the New Testament, while it quotes the Old Testament heavily, never quotes any of these books as Scripture.",
      verses: [
        { ref: "Luke 24:44", text: "And he said unto them, These are the words which I spake unto you, while I was yet with you, that all things must be fulfilled, which were written in the law of Moses, and in the prophets, and in the psalms, concerning me." },
        { ref: "Romans 3:2", text: "Much every way: chiefly, because that unto them were committed the oracles of God." }
      ]
    },
    {
      claim: "Departed righteous souls in heaven pray for and intercede on behalf of the living",
      religion: "Apocrypha",
      keywords: ["intercession of the dead apocrypha", "onias jeremiah vision", "2 maccabees departed saints pray", "saints in heaven intercede"],
      sourceQuote: { ref: "2 Machabees 15:12-14", text: "Onias, who had been high priest, a good and virtuous man... prayed for all the people of the Jews... This is a lover of his brethren, and of the people of Israel: this is he that prayeth much for the people, and for all the holy city, Jeremias, the prophet of God." },
      note: "Judas Machabeus's vision of the deceased Onias and the prophet Jeremiah interceding for Israel from beyond death is cited in support of the intercession of departed saints.",
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "Ecclesiastes 9:5", text: "For the living know that they shall die: but the dead know not any thing, neither have they any more a reward; for the memory of them is forgotten." }
      ]
    },
    {
      claim: "The relics of holy men retain miraculous power even after death",
      religion: "Apocrypha",
      keywords: ["relics power apocrypha", "elisha bones prophesied", "ecclesiasticus relics", "sirach body prophesied after death"],
      sourceQuote: { ref: "Ecclesiasticus 48:14", text: "No word could overcome him, and after death his body prophesied." },
      note: "Said of the prophet Eliseus (Elisha); compare 2 Kings 13:20-21, where a dead man is restored to life on touching Elisha's bones.",
      verses: [
        { ref: "Deuteronomy 34:5-6", text: "So Moses the servant of the LORD died there in the land of Moab... and he buried him... but no man knoweth of his sepulchre unto this day." },
        { ref: "Acts 14:14-15", text: "Which when the apostles, Barnabas and Paul, heard of, they rent their clothes, and ran in among the people, crying out, And saying, Sirs, why do ye these things? We also are men of like passions with you..." }
      ]
    }
  ];

  function escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  // BibleHub-style keyword highlighting: bold/mark every whole-word occurrence
  // of any searched term inside the (already-escaped) verse text.
  function highlightText(safeText, query) {
    const terms = [...new Set(
      query.trim().split(/\s+/).filter(Boolean).map(t => t.toLowerCase())
    )].filter(t => t.length > 1).sort((a, b) => b.length - a.length);

    if (terms.length === 0) return safeText;

    const pattern = terms.map(escapeRegex).join('|');
    const re = new RegExp(`\\b(${pattern})`, 'gi');
    return safeText.replace(re, '<mark class="hl">$1</mark>');
  }

  function renderTopicOverview(query, allMatchedEntries, bibleMatches) {
    const container = document.getElementById('topic-overview');
    const trimmed = query.trim();

    if (trimmed.length < 3) { container.style.display = 'none'; container.innerHTML = ''; return; }

    const religions = [...new Set(allMatchedEntries.map(e => e.religion))];
    const verseMatches = bibleMatches.slice(0, 3).map(m => m.item);

    if (religions.length === 0 && verseMatches.length === 0) {
      container.style.display = 'none';
      container.innerHTML = '';
      return;
    }

    const religionsHtml = religions.length
      ? `<p class="topic-religions">Traditions touching this: ${religions.map(escapeHtml).join(', ')}</p>`
      : `<p class="topic-religions">No tagged tradition entries matched this — showing Scripture only.</p>`;

    const versesHtml = verseMatches.length
      ? `<div class="topic-verses">${verseMatches.map(v => `
          <div class="verse-block">
            <a class="stamp" href="${bibleLink(v.ref)}" target="_blank" rel="noopener noreferrer">${v.ref} ↗</a>
            <p>"${highlightText(escapeHtml(v.text), trimmed)}"</p>
          </div>
        `).join('')}</div>`
      : (bibleIndexReady
          ? ''
          : `<p class="form-hint">Connect once to download the full Bible so Scripture matches show here too.</p>`);

    container.innerHTML = `
      <div class="topic-label">Search</div>
      <h2 class="topic-title">"${escapeHtml(trimmed)}"</h2>
      ${religionsHtml}
      ${versesHtml}
    `;
    container.style.display = 'block';
  }

  const searchInput = document.getElementById('search');
  const chips = document.querySelectorAll('.side-chip');
  const resultsEl = document.getElementById('results');
  const metaEl = document.getElementById('results-meta');
  const emptyEl = document.getElementById('empty');
  const textFilterSelect = document.getElementById('text-filter');
  const paginationEl = document.getElementById('results-pagination');

  let activeFilter = 'all';
  let activeSourceId = null; // narrows activeFilter down to one specific SOURCE_TEXTS entry, via the text-filter dropdown

  // Argument-card pagination: finalizeRender computes the full, sorted match
  // list once per search and caches it here; clicking a dot just re-slices
  // and re-renders from the cache instead of re-running the search.
  const RESULTS_PAGE_SIZE = 7;
  let currentMatches = [];
  let currentResultsPage = 0;

  function renderResultsPagination() {
    if (!paginationEl) return;
    const totalPages = Math.ceil(currentMatches.length / RESULTS_PAGE_SIZE);
    if (totalPages <= 1) {
      paginationEl.style.display = 'none';
      paginationEl.innerHTML = '';
      return;
    }
    paginationEl.style.display = 'flex';
    paginationEl.innerHTML = Array.from({ length: totalPages }, (_, i) =>
      `<button class="page-dot${i === currentResultsPage ? ' active' : ''}" data-page="${i}" aria-label="Page ${i + 1} of ${totalPages}"></button>`
    ).join('');
    paginationEl.querySelectorAll('[data-page]').forEach(dot => {
      dot.addEventListener('click', () => {
        currentResultsPage = parseInt(dot.dataset.page, 10);
        renderResultsPage();
      });
    });
  }

  function renderResultsPage() {
    const start = currentResultsPage * RESULTS_PAGE_SIZE;
    const pageItems = currentMatches.slice(start, start + RESULTS_PAGE_SIZE);
    resultsEl.innerHTML = '';
    pageItems.forEach(entry => resultsEl.appendChild(buildEntryCard(entry)));
    renderResultsPagination();
  }

  function bibleLink(ref) {
    const clean = ref.replace(/\s*\(.*?\)\s*$/, '').trim();
    return `https://www.biblegateway.com/passage/?search=${encodeURIComponent(clean)}&version=KJV`;
  }

  function quranLink(ref) {
    const match = ref.match(/(\d+)\s*:\s*(\d+)/);
    if (!match) return null;
    return `https://quran.com/${match[1]}/${match[2]}`;
  }

  function sourceLink(entry) {
    if (!entry.sourceQuote) return null;
    if (entry.sourceQuote.link) return entry.sourceQuote.link;
    if (/qur'?an/i.test(entry.sourceQuote.ref)) return quranLink(entry.sourceQuote.ref);
    if (/\d+:\d+/.test(entry.sourceQuote.ref) && /^(Genesis|Exodus|Leviticus|Numbers|Deuteronomy|Joshua|Judges|Ruth|Samuel|Kings|Chronicles|Ezra|Nehemiah|Esther|Job|Psalm|Proverbs|Ecclesiastes|Song|Isaiah|Jeremiah|Lamentations|Ezekiel|Daniel|Hosea|Joel|Amos|Obadiah|Jonah|Micah|Nahum|Habakkuk|Zephaniah|Haggai|Zechariah|Malachi|Matthew|Mark|Luke|John|Acts|Romans|Corinthians|Galatians|Ephesians|Philippians|Colossians|Thessalonians|Timothy|Titus|Philemon|Hebrews|James|Peter|Jude|Revelation)/.test(entry.sourceQuote.ref)) {
      return bibleLink(entry.sourceQuote.ref);
    }
    return null;
  }

  const STOPWORDS = new Set(["the","and","are","was","were","that","this","with","from","have","has","had",
    "for","you","your","they","them","into","than","then","who","what","when","where","why","how","does",
    "did","can","cant","can't","could","would","should","just","only","also","very","more","most","which","there","here",
    "about","its","it's","isn't","isnt","doesn't","doesnt","dont","don't","not","did","really",
    "wont","won't","arent","aren't","wasnt","wasn't","werent","weren't","hasnt","hasn't","hadnt","hadn't",
    "wouldnt","wouldn't","couldnt","couldn't","shouldnt","shouldn't","didnt","lets","let's",
    "im","i'm","youre","you're","theyre","they're","thats","that's","whats","what's","people"]);

  function normalizeText(str) {
    return str.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  }

  function tokenize(str) {
    return normalizeText(str).split(' ').filter(w => w.length > 2 && !STOPWORDS.has(w));
  }

  // Bridges everyday search terms to the vocabulary the KJV (1611) actually
  // uses, for topics where the two have essentially no words in common —
  // fuzzy string matching alone can never connect "homosexuality" to
  // "abomination"/"sodomite" no matter how loose the threshold, since they
  // share no letters in common. Each entry's `terms` are checked as a
  // substring of the normalized query; a hit adds that topic's `expand`
  // strings into the broad/fallback search (see buildBroadQuery below) so
  // the actual relevant verses can surface. Curated and intentionally small
  // — add topics here as they come up rather than trying to cover
  // everything up front.
  const TOPIC_SYNONYMS = [
    {
      terms: ["homosexual", "homosexuality", "homosexuals", "gay", "gays", "lesbian", "lesbians", "lgbt", "lgbtq", "same sex", "samesex"],
      expand: ["sodomite", "effeminate", "abomination", "vile affections"]
    },
    {
      terms: ["abortion", "abortions", "aborting", "pro choice", "prochoice"],
      expand: ["womb"]
    },
    {
      terms: ["suicide", "suicidal", "kill myself", "end my life", "want to die"],
      expand: ["brokenhearted", "heavy laden", "comfort", "cast down"]
    },
    {
      terms: ["porn", "porno", "pornography", "pornographic"],
      expand: ["lust", "concupiscence"]
    },
    {
      terms: ["alcoholic", "alcoholics", "alcoholism"],
      expand: ["drunkard", "drunkenness", "strong drink"]
    },
    {
      terms: ["racism", "racist", "racists", "racial discrimination"],
      expand: ["one blood", "respecter of persons", "neither jew nor greek"]
    },
    {
      terms: ["gambling", "gamble", "gambler", "betting", "casino"],
      expand: ["love of money", "hasteth to be rich"]
    },
    {
      terms: ["depression", "depressed", "anxiety", "anxious", "panic attack", "mental health"],
      expand: ["careful for nothing", "take no thought", "casting all your care"]
    }
  ];

  function topicSynonymExpansion(rawQuery) {
    const qNorm = normalizeText(rawQuery);
    const words = new Set();
    TOPIC_SYNONYMS.forEach(topic => {
      if (topic.terms.some(term => qNorm.includes(normalizeText(term)))) {
        topic.expand.forEach(w => words.add(w));
      }
    });
    return [...words];
  }

  // Last-resort fallback for queries that find nothing as either a literal
  // phrase or a spell-corrected phrase — e.g. a full conversational question
  // ("why can't people love each other?"), where the whole raw string will
  // never fuzzy-match any single short verse well, even though a word inside
  // it ("love") is all over the Bible on its own. Reduces the query to its
  // content words (stopwords stripped) plus any curated topic-synonym words,
  // for a per-word OR search (see orSearch in search-worker.js) instead of a
  // whole-phrase fuzzy match.
  function buildBroadQuery(rawQuery) {
    const contentWords = tokenize(rawQuery);
    const topicWords = topicSynonymExpansion(rawQuery);
    const words = [...new Set([...topicWords, ...contentWords])];
    if (words.length === 0) return null;
    return { words, query: words.join(' ') };
  }

  // Curated "vague life question" -> best-verse-answer index. Unlike
  // TOPIC_SYNONYMS above (a last-resort fallback for queries that find
  // NOTHING), this applies on every render, on top of whatever the normal
  // search already found — a question like "how do we get saved?" already
  // fuzzy-matches plenty of verses just fine (the word "saved" is common),
  // the actual gap is that generic fuzzy-match scoring has no way to know
  // which of those hits is the real answer. A match here pins its verses to
  // the very top of the Bible-results list, ahead of whatever the fuzzy
  // ranking would have put there. Every ref below was checked against the
  // live KJV text before being added (see tep_search_topic_synonyms memory
  // for how). Curated and intentionally broad-but-finite — add more here as
  // they come up rather than trying to cover every possible phrasing.
  const TOPICAL_QUESTIONS = [
    { terms: ["how do we get saved", "how can i be saved", "how to be saved", "am i saved", "how do i get saved", "plan of salvation", "how to get to heaven", "how can i get saved"],
      verses: ["Romans 10:9", "Romans 10:13", "Ephesians 2:8", "Ephesians 2:9", "Acts 16:31", "John 3:16"] },
    { terms: ["how do i pray", "how to pray", "how should i pray", "what should i pray for"],
      verses: ["Matthew 6:9", "Philippians 4:6", "1 Thessalonians 5:17", "James 5:16"] },
    { terms: ["how do i forgive", "how to forgive", "why should i forgive", "how can i forgive someone"],
      verses: ["Matthew 6:14", "Matthew 6:15", "Ephesians 4:32", "Colossians 3:13", "Matthew 18:21", "Matthew 18:22"] },
    { terms: ["why does god allow suffering", "why do bad things happen", "why is there evil", "why do we suffer", "why does god allow pain"],
      verses: ["Romans 8:28", "John 16:33", "James 1:2", "James 1:3", "Romans 5:3", "Romans 5:4", "2 Corinthians 4:17"] },
    { terms: ["what happens when we die", "what happens after death", "is there life after death", "what happens when you die"],
      verses: ["John 11:25", "John 11:26", "2 Corinthians 5:8", "Philippians 1:21", "Hebrews 9:27", "Revelation 21:4"] },
    { terms: ["how do i have faith", "i have doubts", "how to believe in god", "how do i believe"],
      verses: ["Hebrews 11:1", "Mark 9:24", "Romans 10:17", "2 Corinthians 5:7"] },
    { terms: ["what is the meaning of life", "what is my purpose", "why am i here", "what is the purpose of life"],
      verses: ["Jeremiah 29:11", "Ecclesiastes 12:13", "Colossians 1:16", "Romans 8:28"] },
    { terms: ["how do i stop worrying", "how to overcome fear", "why am i anxious", "how to deal with anxiety", "how to stop being afraid"],
      verses: ["Philippians 4:6", "Philippians 4:7", "1 Peter 5:7", "Isaiah 41:10", "2 Timothy 1:7"] },
    { terms: ["how do i deal with guilt", "how to overcome sin", "i feel guilty", "how do i stop sinning"],
      verses: ["1 John 1:9", "Romans 8:1", "Psalms 103:12"] },
    { terms: ["does god love me", "how much does god love me", "how do i know god loves me"],
      verses: ["John 3:16", "Romans 5:8", "Romans 8:38", "Romans 8:39", "1 John 4:9", "1 John 4:10"] },
    { terms: ["what does the bible say about marriage", "how should i treat my wife", "how should i treat my husband", "what makes a good marriage"],
      verses: ["Ephesians 5:25", "Ephesians 5:22", "Genesis 2:24", "Colossians 3:19"] },
    { terms: ["why do i feel alone", "how to deal with loneliness", "i feel lonely"],
      verses: ["Deuteronomy 31:6", "Hebrews 13:5", "Psalms 34:18", "Matthew 28:20"] },
    { terms: ["how do i control my anger", "how to stop being angry", "how to deal with anger"],
      verses: ["Ephesians 4:26", "James 1:19", "James 1:20", "Proverbs 15:1", "Proverbs 29:11"] },
    { terms: ["how do i trust god", "what does the future hold", "how do i trust god's plan"],
      verses: ["Proverbs 3:5", "Proverbs 3:6", "Jeremiah 29:11", "Isaiah 41:10"] },
    { terms: ["how do i resist temptation", "how to overcome temptation", "how do i say no to temptation"],
      verses: ["1 Corinthians 10:13", "James 1:12", "James 4:7", "Matthew 26:41"] },
    { terms: ["how do i repent", "what is repentance", "how to repent"],
      verses: ["Acts 3:19", "2 Chronicles 7:14", "1 John 1:9", "Luke 15:7"] },
    { terms: ["why should i be baptized", "what is baptism for", "why get baptized"],
      verses: ["Acts 2:38", "Romans 6:4", "Matthew 28:19"] },
    { terms: ["what is heaven like", "how do i get to heaven", "what will heaven be like"],
      verses: ["John 14:2", "John 14:3", "Revelation 21:4", "Revelation 21:21", "John 3:16"] },
    { terms: ["what is hell", "is hell real", "what is hell like"],
      verses: ["Matthew 25:41", "Revelation 20:15", "Matthew 10:28"] },
    { terms: ["who is the holy spirit", "what does the holy spirit do", "what is the holy spirit's role"],
      verses: ["John 14:26", "Acts 1:8", "Romans 8:26", "Galatians 5:22", "Galatians 5:23"] },
    { terms: ["why should i read the bible", "how do i understand the bible", "how to study the bible"],
      verses: ["2 Timothy 3:16", "Psalms 119:105", "Hebrews 4:12", "Joshua 1:8"] },
    { terms: ["how do i deal with grief", "how to cope with loss", "i lost a loved one"],
      verses: ["Psalms 34:18", "Matthew 5:4", "Revelation 21:4", "2 Corinthians 1:3", "2 Corinthians 1:4"] },
    { terms: ["how do i find peace", "how to have peace of mind", "how do i find inner peace"],
      verses: ["John 14:27", "Philippians 4:7", "Isaiah 26:3"] },
    { terms: ["what does the bible say about money", "should i tithe", "what does the bible say about tithing"],
      verses: ["Matthew 6:24", "1 Timothy 6:10", "Malachi 3:10", "Proverbs 3:9", "Proverbs 3:10"] },
    { terms: ["what does the bible say about work", "how should i work", "what does the bible say about my job"],
      verses: ["Colossians 3:23", "Colossians 3:24", "Proverbs 14:23", "Ecclesiastes 3:13"] },
    { terms: ["how should i raise my children", "what does the bible say about parenting", "how to raise godly children"],
      verses: ["Proverbs 22:6", "Ephesians 6:4", "Deuteronomy 6:6", "Deuteronomy 6:7"] },
    { terms: ["what does the bible say about friendship", "how to be a good friend"],
      verses: ["Proverbs 17:17", "Proverbs 27:17", "John 15:13", "Ecclesiastes 4:9", "Ecclesiastes 4:10"] },
    { terms: ["how do i stay humble", "why is pride bad", "how to overcome pride"],
      verses: ["Proverbs 16:18", "James 4:6", "Philippians 2:3", "Micah 6:8"] },
    { terms: ["how do i make good decisions", "how to get wisdom", "how do i know god's will"],
      verses: ["James 1:5", "Proverbs 3:5", "Proverbs 3:6", "Proverbs 2:6"] },
    { terms: ["when is jesus coming back", "what are the end times", "when will jesus return"],
      verses: ["Matthew 24:36", "1 Thessalonians 4:16", "1 Thessalonians 4:17", "Revelation 22:12"] },
    { terms: ["why go to church", "why is fellowship important", "why do i need church"],
      verses: ["Hebrews 10:24", "Hebrews 10:25", "Matthew 18:20", "Acts 2:42"] },
    { terms: ["how do i love my neighbor", "why should i serve others", "how to love others"],
      verses: ["Mark 12:31", "Galatians 5:13", "1 Peter 4:10", "Matthew 25:40"] },
    { terms: ["how do i be more patient", "how to have patience", "how to wait on god"],
      verses: ["James 1:2", "James 1:3", "James 1:4", "Galatians 5:22", "Romans 5:3", "Romans 5:4"] },
    { terms: ["how to be more thankful", "why should i be grateful", "how do i practice gratitude"],
      verses: ["1 Thessalonians 5:18", "Philippians 4:6", "Psalms 100:4", "Colossians 3:15"] }
  ];

  // Pins a matched topic's verses to the top of the Bible-results list,
  // ahead of whatever the normal fuzzy-match ranking produced (ranking is
  // otherwise pure edit-distance — it has no idea "Romans 10:9" is a better
  // answer to "how do we get saved" than some other verse that happens to
  // contain the word "saved"). Runs on every render, not just the fallback
  // phases, since the underlying search already succeeds fine for most of
  // these questions — this only reorders, never gates on failure first.
  function applyTopicalBoost(rawQuery, bibleResult) {
    if (!rawQuery) return bibleResult;
    const qNorm = normalizeText(rawQuery);
    const boostRefs = [];
    TOPICAL_QUESTIONS.forEach(topic => {
      if (topic.terms.some(term => qNorm.includes(normalizeText(term)))) {
        topic.verses.forEach(ref => { if (!boostRefs.includes(ref)) boostRefs.push(ref); });
      }
    });
    if (boostRefs.length === 0) return bibleResult;

    const boosted = boostRefs
      .map(ref => allBibleVerses.find(v => v.ref === ref))
      .filter(Boolean)
      .map(item => ({ item, score: -1 }));
    if (boosted.length === 0) return bibleResult;

    const boostedRefs = new Set(boosted.map(b => b.item.ref));
    const rest = bibleResult.top.filter(h => !boostedRefs.has(h.item.ref));
    const newlyAdded = boosted.filter(b => !bibleResult.top.some(h => h.item.ref === b.item.ref)).length;

    return {
      total: bibleResult.total + newlyAdded,
      top: [...boosted, ...rest].slice(0, MERGE_TOP_CAP)
    };
  }

  /* ===== Typo-tolerant word correction ===== */

  const EXTRA_VOCAB = [
    "resurrection","crucifixion","salvation","reincarnation","trinity","atonement","forgiveness",
    "repentance","baptism","prophecy","scripture","covenant","commandments","sabbath","genesis",
    "creation","judgment","righteousness","sanctification","justification","redemption","grace",
    "gospel","apostle","disciple","messiah","prophet","idolatry","blasphemy","heresy","doctrine",
    "eternity","damnation","tribulation","rapture","millennium","antichrist","satan","demon",
    "angel","archangel","paradise","purgatory","reconciliation","intercession","omniscience",
    "omnipotence","omnipresence","incarnation","transfiguration","ascension","pentecost",
    "circumcision","sacrifice","tabernacle","synagogue","pharisee","sadducee","gentile","israelite",
    "patriarch","apostasy","predestination","providence","theodicy","eschatology","soteriology",
    "christology","pneumatology","hermeneutics","exegesis","monotheism","polytheism","pantheism",
    "agnosticism","secularism","materialism","nihilism","existentialism","enlightenment",
    "nirvana","karma","samsara","dharma","moksha","brahman","atman","tao","zen","sutra"
  ];

  const VOCAB = (() => {
    const words = new Set();
    ENTRIES.forEach(e => {
      tokenize(e.claim).forEach(w => words.add(w));
      e.keywords.forEach(k => tokenize(k).forEach(w => words.add(w)));
      if (e.positionSummary) tokenize(e.positionSummary).forEach(w => words.add(w));
    });
    EXTRA_VOCAB.forEach(w => words.add(w));
    return [...words];
  })();

  // Populated once the full Bible loads — gives us a large real-English dictionary
  // so ordinary words ("fire", "water", "mercy") are never "corrected" into something else.
  let realWordSet = new Set(VOCAB);

  function buildRealWordSet(verses) {
    const s = new Set(VOCAB);
    verses.forEach(v => {
      normalizeText(v.text).split(' ').forEach(w => { if (w.length > 2) s.add(w); });
    });
    realWordSet = s;
  }

  function levenshtein(a, b) {
    const m = a.length, n = b.length;
    if (m === 0) return n;
    if (n === 0) return m;
    const prev = new Array(n + 1);
    const curr = new Array(n + 1);
    for (let j = 0; j <= n; j++) prev[j] = j;
    for (let i = 1; i <= m; i++) {
      curr[0] = i;
      for (let j = 1; j <= n; j++) {
        const cost = a[i - 1] === b[j - 1] ? 0 : 1;
        curr[j] = Math.min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost);
      }
      for (let j = 0; j <= n; j++) prev[j] = curr[j];
    }
    return prev[n];
  }

  function correctWord(word) {
    if (word.length < 4) return word;
    // If it's a real word (in Scripture or our vocab), never touch it.
    if (realWordSet.has(word)) return word;
    // Conservative: only fix small typos, and only toward app-relevant vocabulary.
    const maxDist = word.length <= 6 ? 1 : 2;
    let best = null;
    let bestDist = maxDist + 1;
    for (const v of VOCAB) {
      if (Math.abs(v.length - word.length) > maxDist) continue;
      const d = levenshtein(word, v);
      if (d < bestDist) { bestDist = d; best = v; }
    }
    return (best && bestDist <= maxDist) ? best : word;
  }

  function correctQuery(query) {
    const words = query.trim().split(/\s+/).filter(Boolean);
    let changed = false;
    const corrected = words.map(w => {
      const lower = w.toLowerCase();
      const fixed = correctWord(lower);
      if (fixed !== lower) changed = true;
      return fixed;
    });
    return { corrected: corrected.join(' '), changed, original: query };
  }

  function score(entry, query) {
    if (!query) return 1;
    const qNorm = normalizeText(query);
    const qTokens = tokenize(query);
    if (qTokens.length === 0) return 1;

    const extraText = entry.sourceQuote ? entry.sourceQuote.text : (entry.positionSummary || '');
    const fieldsNorm = normalizeText([entry.claim, entry.religion, extraText].join(' '));

    let total = 0;

    entry.keywords.forEach(k => {
      const kNorm = normalizeText(k);
      if (kNorm.length < 4) return;
      if (qNorm === kNorm) { total += 20; return; }
      if (qNorm.includes(kNorm) || kNorm.includes(qNorm)) { total += 10; return; }
      const kTokens = tokenize(k);
      if (kTokens.length > 1) {
        const overlap = kTokens.filter(t => qTokens.includes(t)).length;
        if (overlap === kTokens.length) total += 8;
        else if (overlap > 0) total += overlap * 2;
      }
    });

    if (qNorm.length > 3 && fieldsNorm.includes(qNorm)) total += 15;
    const claimTokens = tokenize(entry.claim);
    const claimOverlap = qTokens.filter(t => claimTokens.includes(t)).length;
    total += claimOverlap * 3;

    const fieldTokenOverlap = qTokens.filter(t => fieldsNorm.includes(t)).length;
    total += fieldTokenOverlap;

    return total;
  }

  function buildEntryCard(entry) {
    const card = document.createElement('div');
    card.className = 'case';
    const srcLink = sourceLink(entry);
    const srcStamp = srcLink
      ? `<a class="stamp" href="${srcLink}" target="_blank" rel="noopener noreferrer">${entry.sourceQuote.ref} ↗</a>`
      : `<span class="stamp">${entry.sourceQuote ? entry.sourceQuote.ref : ''}</span>`;
    const claimContent = entry.sourceQuote
      ? `<div class="verse-block">
           ${srcStamp}
           <p>"${entry.sourceQuote.text}"</p>
         </div>${entry.note ? `<p class="teaching" style="margin-top:10px;font-size:13px;"><em>Context:</em> ${entry.note}</p>` : ''}`
      : `<p class="teaching"><em>No single canonical line to quote here.</em><br>${entry.positionSummary}</p>`;
    const eid = entryId(entry);
    const isBm = bookmarkIds.has(eid);
    card.innerHTML = `
      <div class="case-head">
        <h2>${entry.claim}</h2>
        <div class="card-actions">
          <span class="religion-tag">${entry.religion}</span>
          <button class="icon-btn share-btn" data-eid="${eid}" title="Share" aria-label="Share">⇪</button>
          <button class="icon-btn bookmark-btn ${isBm ? 'bookmarked' : ''}" data-eid="${eid}" title="Bookmark" aria-label="Bookmark">${isBm ? '★' : '☆'}</button>
        </div>
      </div>
      <div class="case-body">
        <div class="panel claim-panel">
          <div class="panel-label">Their own words</div>
          ${claimContent}
        </div>
        <div class="panel answer-panel">
          <div class="panel-label">Scripture's answer</div>
          ${entry.verses.map(v => `
            <div class="verse-block">
              <a class="stamp" href="${bibleLink(v.ref)}" target="_blank" rel="noopener noreferrer">${v.ref} ↗</a>
              <p>"${v.text}"</p>
            </div>
          `).join('')}
        </div>
      </div>
    `;
    card.querySelector('.bookmark-btn').addEventListener('click', () => toggleBookmark(entry));
    card.querySelector('.share-btn').addEventListener('click', () => shareEntry(entry));
    return card;
  }

  function entriesFor(q) {
    return q ? ENTRIES.filter(e => score(e, q) > 0) : [];
  }

  function currentSourceIdWhitelist() {
    if (activeSourceId) return sourceStatus[activeSourceId] === 'ready' ? [activeSourceId] : [];
    if (activeFilter === 'all') return null;
    return SOURCE_TEXTS.filter(s => s.tradition === activeFilter && sourceStatus[s.id] === 'ready').map(s => s.id);
  }

  // The worker processes messages strictly one at a time, in the order it
  // receives them, with no concept of "stale" — it will happily compute a
  // search for every keystroke of a fast typist even though only the last
  // one ever gets rendered. Left unchecked, that backlog is exactly what
  // turned a ~1.5s search into a 10+ second wait: 15 queued searches ahead
  // of the one you actually typed, each one blocking the next.
  //
  // The fix is a hard cap of one search in flight at a time. A new request
  // that arrives while one is still running doesn't queue behind it — it
  // just overwrites whatever was queued, so only the latest ever waits, and
  // it fires the instant the current search returns.
  let searchInFlight = false;
  let queuedSearch = null; // { rawQuery, query, phase, orWords } — most recent request received while one was in flight
  let searchDispatchedAt = 0;

  // Accumulates one reqId's replies from every pool worker before the search
  // is considered done — see handlePoolMessage. Only one in flight at a time
  // (searchInFlight/queuedSearch above), so a single pending record suffices.
  let poolPending = null;

  // orWords: set only for the 'broad' fallback phase (see buildBroadQuery) —
  // tells the worker to OR-match each word individually instead of fuzzy-
  // matching the whole query string as one pattern. Absent/null for every
  // other phase, which keeps their existing whole-string matching behavior.
  function dispatchSearch(rawQuery, query, phase, orWords) {
    const reqId = ++searchReqSeq;
    latestSearchReqId = reqId;
    searchInFlight = true;
    searchDispatchedAt = performance.now();
    poolPending = {
      reqId, rawQuery, query, phase,
      remaining: searchWorkers.length,
      bible: EMPTY_MATCHES,
      sourceTotal: 0,
      sourceParts: [],
      bibleMs: 0,
      maxSourceMs: 0,
      perSourceMs: {}
    };
    const sourceIdWhitelist = currentSourceIdWhitelist();
    searchWorkers.forEach(w => w.postMessage({ type: 'search', reqId, phase, rawQuery, query, sourceIdWhitelist, orWords: orWords || null }));
  }

  function requestSearch(rawQuery, query, phase, orWords) {
    if (searchInFlight) {
      queuedSearch = { rawQuery, query, phase, orWords: orWords || null };
      latestSearchReqId = ++searchReqSeq; // the in-flight response, once it arrives, is now stale
      return;
    }
    dispatchSearch(rawQuery, query, phase, orWords);
  }

  function render() {
    const rawQuery = searchInput.value.trim();

    // Below ~2 characters a fuzzy search across tens of thousands of rows is
    // mostly noise anyway — skip dispatching it and save the wasted work.
    if (!rawQuery || rawQuery.length < 2 || !searchWorkers.length) {
      queuedSearch = null;
      latestSearchReqId = ++searchReqSeq; // invalidate any in-flight/queued response from a previous call
      finalizeRender(rawQuery, rawQuery, false, entriesFor(rawQuery), EMPTY_MATCHES, EMPTY_MATCHES);
      return;
    }

    requestSearch(rawQuery, rawQuery, 'original');
  }

  // Each pool worker replies once per reqId with its own slice of results
  // (bible only ever comes from worker 0 — see loadBibleIntoWorkers). This
  // accumulates every worker's reply for the current reqId and only moves
  // on once all of them are in, same "one search in flight" guarantee the
  // single-worker version had, just fanned out across the pool.
  function handlePoolMessage(workerIndex, e) {
    const msg = e.data;
    if (msg.type !== 'search-result') return;
    if (!poolPending || msg.reqId !== poolPending.reqId) return; // stale — a newer request has since superseded this

    if (msg.bible.total > 0 || workerIndex === 0) poolPending.bible = msg.bible;
    poolPending.sourceTotal += msg.source.total;
    poolPending.sourceParts.push(msg.source.top);
    if (msg.timing) {
      poolPending.bibleMs = Math.max(poolPending.bibleMs, msg.timing.bibleMs);
      poolPending.maxSourceMs = Math.max(poolPending.maxSourceMs, msg.timing.sourceMs);
      Object.assign(poolPending.perSourceMs, msg.timing.perSourceMs);
    }
    settlePoolReply();
  }

  // A worker that throws (a bad Fuse.js edge case, an out-of-memory hiccup,
  // whatever) must still count toward completion — otherwise `remaining`
  // never reaches 0, searchInFlight is stuck true forever, and every search
  // after that point just queues up behind it and never fires again. Losing
  // that one worker's shard of results for this query is an acceptable
  // trade for "search still works at all."
  function handlePoolWorkerError(workerIndex, err) {
    console.warn(`TEP: search worker ${workerIndex} errored:`, err && err.message ? err.message : err);
    if (!poolPending) return; // no search currently in flight — nothing to unstick
    settlePoolReply();
  }

  function settlePoolReply() {
    poolPending.remaining--;
    if (poolPending.remaining > 0) return; // still waiting on the rest of the pool

    const merged = poolPending;
    poolPending = null;

    searchInFlight = false;
    if (queuedSearch) {
      const next = queuedSearch;
      queuedSearch = null;
      dispatchSearch(next.rawQuery, next.query, next.phase, next.orWords);
    }

    if (merged.reqId !== latestSearchReqId) return; // stale — a newer request has since superseded this response

    const roundTripMs = +(performance.now() - searchDispatchedAt).toFixed(1);
    console.log(`[render] "${merged.query}" (${merged.phase}) — round trip ${roundTripMs}ms across ${searchWorkers.length} workers (bible ${merged.bibleMs}ms, slowest source shard ${merged.maxSourceMs}ms)`, merged.perSourceMs);

    const mergedSource = {
      total: merged.sourceTotal,
      top: merged.sourceParts.flat().sort((a, b) => (a.score ?? 0) - (b.score ?? 0)).slice(0, MERGE_TOP_CAP)
    };
    finishSearchResult(merged.rawQuery, merged.query, merged.phase, merged.bible, mergedSource);
  }

  // Falls back to a per-word OR search (see buildBroadQuery) when neither the
  // literal query nor its spell-corrected form found anything. Returns true
  // if that fallback was dispatched (caller should return without rendering
  // yet), false if there was nothing useful to try.
  function tryBroadFallback(rawQuery) {
    const broad = buildBroadQuery(rawQuery);
    if (!broad) return false;
    requestSearch(rawQuery, broad.query, 'broad', broad.words);
    return true;
  }

  // Only reach for spelling correction / broad fallback if the literal query
  // found nothing anywhere (no claim entries, no Bible verses) — see
  // finalizeRender for where a successful/failed attempt ends up rendered.
  function finishSearchResult(rawQuery, query, phase, bible, source) {
    if (phase === 'original') {
      const entries = entriesFor(query);
      if (entries.length === 0 && bible.total === 0) {
        pendingOriginalSource = source;
        const attempt = correctQuery(query);
        if (attempt.changed) {
          requestSearch(rawQuery, attempt.corrected, 'correction');
          return;
        }
        if (tryBroadFallback(rawQuery)) return;
      }
      finalizeRender(rawQuery, query, false, entries, bible, source);
      return;
    }

    if (phase === 'correction') {
      const entries = entriesFor(query);
      if (entries.length > 0 || bible.total > 0) {
        finalizeRender(rawQuery, query, true, entries, bible, source);
        return;
      }
      if (tryBroadFallback(rawQuery)) return;
      // Correction didn't help either — fall back to the original query's
      // own source-text matches (if any), which are still valid even though
      // nothing matched among claim entries or the Bible.
      finalizeRender(rawQuery, rawQuery, false, [], EMPTY_MATCHES, pendingOriginalSource);
      return;
    }

    // phase === 'broad'
    const entries = entriesFor(query);
    if (entries.length > 0 || bible.total > 0) {
      // Show the user's own words as the label, not the internal OR-word
      // list used to actually drive the match.
      finalizeRender(rawQuery, query, true, entries, bible, source, rawQuery);
    } else {
      finalizeRender(rawQuery, rawQuery, false, [], EMPTY_MATCHES, pendingOriginalSource);
    }
  }

  // displayLabel: overrides what's shown in the "N matches for X" text —
  // used only by the broad-fallback phase, where `query` itself is an
  // internal OR-word list (e.g. "homosexuality sodomite effeminate
  // abomination") that's fine for matching/highlighting but would be
  // confusing to show the user verbatim. Every other phase leaves this
  // unset and just shows `query` as before.
  function finalizeRender(rawQuery, query, corrected, allMatchedEntries, bibleResult, sourceResult, displayLabel) {
    const boostedBible = applyTopicalBoost(rawQuery, bibleResult);
    renderTopicOverview(query, allMatchedEntries, boostedBible.top);
    renderBibleResults(query, boostedBible);
    renderSourceResults(query, sourceResult);

    const label = displayLabel || query;

    let matches = ENTRIES
      .map(e => ({ entry: e, s: score(e, query) }))
      .filter(m => m.s > 0)
      .filter(m => activeFilter === 'all' || m.entry.religion === activeFilter)
      .sort((a, b) => b.s - a.s)
      .map(m => m.entry);

    const correctionNote = corrected
      ? `<span class="correction-note">Showing results for "${escapeHtml(label)}"</span>`
      : '';

    if (matches.length === 0) {
      currentMatches = [];
      currentResultsPage = 0;
      resultsEl.innerHTML = '';
      renderResultsPagination();
      emptyEl.style.display = 'block';
      metaEl.innerHTML = correctionNote;
      return;
    }
    emptyEl.style.display = 'none';
    metaEl.innerHTML = rawQuery
      ? `${matches.length} match${matches.length === 1 ? '' : 'es'} for "${escapeHtml(label)}" ${correctionNote}`
      : `${matches.length} entries`;

    // A fresh search/filter always starts back on page 1, even if a stale
    // page number was left over from a previous, longer result set.
    currentMatches = matches;
    currentResultsPage = 0;
    renderResultsPage();

    if (rawQuery) {
      matches.slice(0, 5).forEach(entry => logSearchHit(entryId(entry)));
    }
  }

  // The actual Fuse.js search now runs off-thread in search-worker.js, so
  // typing can never be blocked by it. This debounce reduces how many
  // throwaway searches get dispatched during a typing burst — the in-flight
  // cap above is what guarantees no backlog, this just cuts how often it
  // needs to kick in.
  let renderDebounceTimer = null;
  function scheduleRender() {
    clearTimeout(renderDebounceTimer);
    renderDebounceTimer = setTimeout(render, 150);
  }
  searchInput.addEventListener('input', scheduleRender);

  // Repopulates the text-filter dropdown for whichever tradition is now
  // active. Hidden entirely for "All" and for any tradition with fewer than
  // two source texts (Islam's six hadith collections need it; Apocrypha's
  // single deuterocanon file doesn't — a dropdown with only one real choice
  // besides "All" is just noise).
  function updateTextFilterForTradition(tradition) {
    activeSourceId = null;
    if (!textFilterSelect) return;
    const matching = tradition === 'all' ? [] : SOURCE_TEXTS.filter(s => s.tradition === tradition);
    if (matching.length < 2) {
      textFilterSelect.style.display = 'none';
      textFilterSelect.innerHTML = '';
      return;
    }
    textFilterSelect.innerHTML = `<option value="">All ${escapeHtml(tradition)} texts</option>` +
      matching.map(s => `<option value="${escapeHtml(s.id)}">${escapeHtml(s.label)}</option>`).join('');
    textFilterSelect.value = '';
    textFilterSelect.style.display = '';
  }
  if (textFilterSelect) {
    textFilterSelect.addEventListener('change', () => {
      activeSourceId = textFilterSelect.value || null;
      render();
    });
  }

  chips.forEach(chip => {
    chip.addEventListener('click', () => {
      chips.forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      activeFilter = chip.dataset.filter;
      updateTextFilterForTradition(activeFilter);
      render();
    });
  });

  const religionSearch = document.getElementById('religion-search');
  const sidebarEmpty = document.getElementById('sidebar-empty');
  religionSearch.addEventListener('input', () => {
    const q = religionSearch.value.trim().toLowerCase();
    let anyVisible = false;
    chips.forEach(chip => {
      const isAll = chip.dataset.filter === 'all';
      const matches = isAll || chip.textContent.toLowerCase().includes(q);
      chip.style.display = matches ? '' : 'none';
      if (matches) anyVisible = true;
    });
    // Hide a whole group (including its label) when the search leaves none
    // of its chips visible, so e.g. "Cults / Sects" doesn't show as an
    // empty header while every chip beneath it is filtered out.
    document.querySelectorAll('.sidebar-group').forEach(group => {
      const hasVisible = [...group.querySelectorAll('.side-chip')].some(c => c.style.display !== 'none');
      group.style.display = hasVisible ? '' : 'none';
    });
    sidebarEmpty.style.display = anyVisible ? 'none' : 'block';
  });

  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('./sw.js').catch(() => {});
    });
  }

  /* ================= Other traditions' source texts ================= */
  /*
   * Each source is fetched at runtime, cached in IndexedDB, and searched offline
   * alongside Scripture. Every source below is public domain or openly licensed —
   * no API keys, no redistribution restrictions.
   *
   * `urls` is a fallback chain: the first URL that responds and parses wins.
   * `parse` is deliberately defensive: it normalizes whatever shape comes back
   * into a flat [{ ref, text }] list. If a source fails entirely, the app skips
   * it and reports it in Settings — it never fails silently or breaks the app.
   */

  // Sutta titles for Digha Nikaya (1-34) and Majjhima Nikaya (1-152), used only
  // to label "Read Full Text" groups — the canonical English titles from
  // Sujato's translation, index 0 unused (suttas are numbered from 1).
  const DN_TITLES = [null, "The Divine Net","The Fruits of the Ascetic Life","With Ambaṭṭha","With Soṇadaṇḍa","With Kūṭadanta","With Mahāli","With Jāliya","The Lion’s Roar to the Naked Ascetic Kassapa","With Poṭṭhapāda","With Subha","With Kevaḍḍha","With Lohicca","Experts in the Three Vedas","The Great Discourse on Traces Left Behind","The Great Discourse on Causation","The Great Discourse on the Buddha’s Extinguishment","King Mahāsudassana","With Janavasabha","The Great Steward","The Great Congregation","Sakka’s Questions","The Longer Discourse on Mindfulness Meditation","With Pāyāsi","About Pāṭikaputta","The Lion’s Roar at the Monastery of Lady Udumbarikā","The Wheel-Turning Monarch","What Came First","Inspiring Confidence","An Impressive Discourse","The Marks of a Great Man","Advice to Sigālaka","The Āṭānāṭiya Protection","Reciting in Concert","Up to Ten"
  ];
  // Shared by the sn-an-highlights source (fetchRows) and parseReadRef,
  // so there's exactly one list to keep in sync, not two.
  const SN_AN_HIGHLIGHTS = [
    { path: 'sn/sn56/sn56.11', prefix: 'sn56.11', label: 'Samyutta Nikaya 56.11 (Setting the Wheel of Dhamma in Motion)' },
    { path: 'sn/sn22/sn22.59', prefix: 'sn22.59', label: 'Samyutta Nikaya 22.59 (The Characteristic of Non-Self)' },
    { path: 'sn/sn35/sn35.28', prefix: 'sn35.28', label: 'Samyutta Nikaya 35.28 (The Fire Sermon)' },
    { path: 'sn/sn12/sn12.2', prefix: 'sn12.2', label: 'Samyutta Nikaya 12.2 (Analysis of Dependent Origination)' },
    { path: 'sn/sn45/sn45.8', prefix: 'sn45.8', label: 'Samyutta Nikaya 45.8 (Analysis of the Noble Eightfold Path)' },
    { path: 'sn/sn56/sn56.31', prefix: 'sn56.31', label: 'Samyutta Nikaya 56.31 (The Simsapa Leaves)' },
    { path: 'an/an3/an3.65', prefix: 'an3.65', label: 'Anguttara Nikaya 3.65 (With the Kalamas)' }
  ];

  const MN_TITLES = [null, "The Root of All Things","All the Defilements","Heirs in the Teaching","Fear and Dread","Unblemished","One Might Wish","The Simile of the Cloth","Self-Effacement","Right View","Mindfulness Meditation","The Shorter Discourse on the Lion’s Roar","The Longer Discourse on the Lion’s Roar","The Longer Discourse on the Mass of Suffering","The Shorter Discourse on the Mass of Suffering","Measuring Up","Hard-heartedness","Jungle Thickets","The Honey-Cake","Two Kinds of Thought","How to Stop Thinking","The Simile of the Saw","The Simile of the Cobra","The Termite Mound","Chariots at the Ready","Sowing","The Noble Quest","The Shorter Simile of the Elephant’s Footprint","The Longer Simile of the Elephant’s Footprint","The Longer Simile of the Heartwood","The Shorter Simile of the Heartwood","The Shorter Discourse at Gosiṅga","The Longer Discourse at Gosiṅga","The Longer Discourse on the Cowherd","The Shorter Discourse on the Cowherd","The Shorter Discourse With Saccaka","The Longer Discourse With Saccaka","The Shorter Discourse on the Ending of Craving","The Longer Discourse on the Ending of Craving","The Longer Discourse at Assapura","The Shorter Discourse at Assapura","The People of Sālā","The People of Verañjā","The Great Elaboration","The Shorter Elaboration","The Shorter Discourse on Taking Up Practices","The Great Discourse on Taking Up Practices","The Inquirer","The Mendicants of Kosambī","On the Invitation of Divinity","The Condemnation of Māra","With Kandaraka","The Wealthy Citizen","A Trainee","With Potaliya the Householder","With Jīvaka","With Upāli","The Ascetic Who Behaved Like a Dog","With Prince Abhaya","The Many Kinds of Feeling","A Sure Bet","Advice to Rāhula at Ambalaṭṭhika","The Longer Advice to Rāhula","The Shorter Discourse With Māluṅkyaputta","The Longer Discourse With Māluṅkya","With Bhaddāli","The Simile of the Quail","At Cātumā","At Naḷakapāna","With Gulissāni","At Kīṭāgiri","To Vacchagotta on the Three Knowledges","With Vacchagotta on Fire","The Longer Discourse With Vacchagotta","With Dīghanakha","With Māgaṇḍiya","With Sandaka","The Longer Discourse with Sakuludāyī","With Uggāhamāna Samaṇamaṇḍikāputta","The Shorter Discourse With Sakuludāyī","With Vekhanasa","With Ghaṭīkāra","With Raṭṭhapāla","About King Maghadeva","At Madhurā","With Prince Bodhi","With Aṅgulimāla","Born From the Beloved","The Imported Cloth","Shrines to the Teaching","At Kaṇṇakatthala","With Brahmāyu","With Sela","With Assalāyana","With Ghoṭamukha","With Caṅkī","With Esukārī","With Dhanañjāni","With Vāseṭṭha","With Subha","With Saṅgārava","At Devadaha","The Five and Three","Is This What You Think Of Me?","At Sāmagāma","With Sunakkhatta","Conducive to the Imperturbable","With Moggallāna the Accountant","With Moggallāna the Guardian","The Longer Discourse on the Full-Moon Night","The Shorter Discourse on the Full-Moon Night","One by One","The Sixfold Purification","A True Person","What Should and Should Not Be Cultivated","Many Elements","At Isigili","The Great Forty","Mindfulness of Breathing","Mindfulness of the Body","Rebirth by Choice","The Shorter Discourse on Emptiness","The Longer Discourse on Emptiness","Incredible and Amazing","With Bakkula","The Level of the Tamed","With Bhūmija","With Anuruddha","Corruptions","The Foolish and the Astute","Messengers of the Gods","One Fine Night","Ānanda and One Fine Night","Mahākaccāna and One Fine Night","Lomasakaṅgiya and One Fine Night","The Shorter Analysis of Deeds","The Longer Analysis of Deeds","The Analysis of the Six Sense Fields","A Summary Recital and its Analysis","The Analysis of No Strife","The Analysis of the Elements","The Analysis of the Truths","The Analysis of Religious Donations","Advice to Anāthapiṇḍika","Advice to Channa","Advice to Puṇṇa","Advice from Nandaka","The Shorter Advice to Rāhula","Six By Six","The Great Discourse on What Relates to the Six Sense Fields","With the People of Nagaravinda","The Purification of Alms","The Development of the Faculties"
  ];

  const SOURCE_TEXTS = [
    {
      id: 'quran',
      label: "Qur'an",
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/quran-api',
      /*
       * The editions index is an object keyed by edition id:
       *   { "eng-abdullahyusufal": { name, author, language: "English", link, linkmin }, ... }
       * It hands us the real file URL in `link`/`linkmin`, so we use that rather than
       * building a URL ourselves — no guessing at translation slugs.
       */
      discover: async () => {
        const INDEX_URLS = [
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions.min.json',
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions.json'
        ];

        for (const indexUrl of INDEX_URLS) {
          try {
            const res = await fetch(indexUrl);
            if (!res.ok) continue;
            const editions = await res.json();
            const list = Array.isArray(editions) ? editions : Object.values(editions);

            // Plain English editions only — skip latin-script transliterations (-la / -lad).
            const english = list.filter(e =>
              e && typeof e === 'object' &&
              typeof e.language === 'string' &&
              e.language.trim().toLowerCase() === 'english' &&
              typeof e.name === 'string' &&
              !/-la(d)?$/.test(e.name)
            );
            if (!english.length) continue;

            // Prefer well-known public-domain translations, else take the first English one.
            const preferred = ['eng-abdullahyusufal', 'eng-mohammadhabibsh', 'eng-mohammedmarmadu'];
            const pick = english.find(e => preferred.includes(e.name)) || english[0];

            const urls = [pick.linkmin, pick.link].filter(u => typeof u === 'string' && u);
            if (urls.length) return urls;
          } catch (err) {
            // try next index URL
          }
        }

        // Last resort if the index itself is unreachable.
        return [
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/eng-abdullahyusufal.min.json',
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/eng-abdullahyusufal.json'
        ];
      },
      parse: (data) => normalizeVerseList(data, (row, i) => {
        const ch = row.chapter ?? row.surah ?? row.sura;
        const vs = row.verse ?? row.ayah ?? row.aya;
        return (ch != null && vs != null) ? `Qur'an ${ch}:${vs}` : `Qur'an ${i + 1}`;
      }, { requireLatin: true })
    },
    {
      id: 'gita',
      label: 'Bhagavad Gita',
      tradition: 'Hinduism',
      license: 'Unlicense (public domain dedication) — gita/gita',
      /*
       * VERIFIED: data/verse.json exists and returns a bare array whose rows carry
       * chapter_id / chapter_number. In this repo the verse rows hold the Sanskrit,
       * while English lives in a sibling translation.json joined by verse id — so we
       * fetch both and stitch them. If translation.json is missing or shaped
       * differently, we fall back to any Latin-script text already on the verse row.
       */
      fetchRows: async () => {
        const BASE = 'https://cdn.jsdelivr.net/gh/gita/gita@c6fce39595445768876ddbb8d1268a9c935e1d2b/data';

        const getJson = async (url) => {
          const res = await fetch(url);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json();
        };

        const asArray = (d) => {
          if (Array.isArray(d)) return d;
          if (d && typeof d === 'object') {
            for (const v of Object.values(d)) {
              if (Array.isArray(v) && v.length && typeof v[0] === 'object') return v;
            }
          }
          return [];
        };

        const verses = asArray(await getJson(`${BASE}/verse.json`));
        if (!verses.length) throw new Error('verse.json returned no rows');

        // Try to pull English translations and index them by verse id.
        const englishByVerseId = {};
        try {
          const translations = asArray(await getJson(`${BASE}/translation.json`));
          translations.forEach(t => {
            if (!t || typeof t !== 'object') return;
            const lang = String(t.lang ?? t.language ?? '').toLowerCase();
            if (lang && !lang.startsWith('en')) return;   // English only
            const vid = t.verse_id ?? t.verseId ?? t.verse ?? t.id;
            if (vid == null) return;
            const text = t.description ?? t.translation ?? t.text ?? t.content;
            if (typeof text !== 'string' || !text.trim()) return;
            // Keep the first English translation we see per verse.
            if (!englishByVerseId[vid]) englishByVerseId[vid] = text.trim();
          });
        } catch (err) {
          // translation.json unavailable — fall through to verse-row text below.
        }

        const isMostlyLatin = (s) => {
          const letters = s.replace(/[^A-Za-z\u0900-\u097F\u0600-\u06FF]/g, '');
          if (!letters.length) return false;
          return (letters.match(/[A-Za-z]/g) || []).length / letters.length > 0.6;
        };

        const rows = verses.map((v) => {
          const ch = v.chapter_number ?? v.chapter_id ?? v.chapter;
          const vs = v.verse_number ?? v.verse_order ?? v.verse;
          const vid = v.id ?? v.verse_id;

          let text = (vid != null && englishByVerseId[vid]) || null;

          // Fallback: an English field already present on the verse row.
          if (!text) {
            for (const k of ['translation', 'english', 'meaning', 'text']) {
              const val = v[k];
              if (typeof val === 'string' && val.trim() && isMostlyLatin(val)) { text = val.trim(); break; }
            }
          }
          if (!text) return null;

          const ref = (ch != null && vs != null)
            ? `Bhagavad Gita ${ch}:${vs}`
            : `Bhagavad Gita ${vid ?? '?'}`;
          return { ref, text: stripHtml(text) };
        }).filter(Boolean);

        if (!rows.length) throw new Error('no English text found (verse.json is Sanskrit; translation.json join failed)');
        return rows;
      }
    },
    /*
     * Yoga Sutras of Patanjali — Charles Johnston's 1912 translation,
     * public domain. 195 sutras across the 4 traditional padas. Only the
     * translated sutra text is kept, not Johnston's verse-by-verse mystical
     * commentary.
     */
    {
      id: 'yoga-sutras',
      label: 'Yoga Sutras of Patanjali',
      tradition: 'Hinduism',
      license: 'Public domain — Charles Johnston translation (1912)',
      urls: ['./yoga-sutras.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Brahma Sutras — George Thibaut's translation (Sacred Books of the
     * East vols. 34 and 38, Shankara's commentary), public domain. Only
     * each sutra's own one-sentence translation is kept, not the
     * surrounding paragraphs of Shankara's commentary/disputation (same
     * principle as Summa Theologica below — keep the core statement, not
     * the argument around it). 545 of the traditional ~555 sutras are
     * present; the remainder could not be cleanly isolated from
     * surrounding commentary in the source scan (severe OCR corruption of
     * the sutra number itself in a handful of spots) and are simply
     * absent rather than guessed at.
     */
    {
      id: 'brahma-sutras',
      label: 'Brahma Sutras',
      tradition: 'Hinduism',
      license: 'Public domain — George Thibaut translation, Sacred Books of the East vols. 34 & 38',
      urls: ['./brahma-sutras.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Mahabharata — Kisari Mohan Ganguli's prose translation
     * (1883-96), the only complete English translation in the public
     * domain, via sacred-texts.com's Distributed Proofreaders text
     * (mirrored at github.com/aasi-archive/mbh). One row per numbered
     * section (adhyaya) rather than per verse — Ganguli's translation is
     * continuous prose, not individually verse-numbered like the Gita.
     * 2,110 sections across all 18 parvas.
     */
    {
      id: 'mahabharata',
      label: 'The Mahabharata',
      tradition: 'Hinduism',
      license: 'Public domain — Kisari Mohan Ganguli translation (1883-96)',
      urls: ['./mahabharata.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The 13 principal Upanishads — Robert Ernest Hume's 1921 translation
     * ("The Thirteen Principal Upanishads", Oxford University Press),
     * public domain — the one translation covering exactly this canonical
     * set of 13 in one voice, including Mandukya (which Max Müller's
     * earlier SBE edition omits). Verses are numbered as one flat
     * continuous count per Upanishad rather than reconstructing the full
     * traditional adhyaya/brahmana/valli/khanda nesting (which varies by
     * text and isn't consistently machine-derivable from this scan) —
     * same simplification this app already uses for the Dhammapada,
     * which also doesn't preserve its traditional vagga divisions.
     */
    {
      id: 'upanishads',
      label: 'The Principal Upanishads',
      tradition: 'Hinduism',
      license: 'Public domain — Robert Ernest Hume translation (1921)',
      urls: ['./upanishads.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Ramayana — Ralph T.H. Griffith's verse translation (1870-74),
     * public domain, via Project Gutenberg. One row per Canto (Griffith's
     * translation is continuous verse, not individually shloka-numbered).
     * Covers Books I-VI (Bala, Ayodhya, Aranya, Kishkindha, Sundara,
     * Yuddha) — the six books universally attributed to the core epic;
     * Griffith's own translation does not include Uttara Kanda, which is
     * widely regarded by scholars as a later addition to the text, so
     * it's absent here rather than patched in from a different translator
     * (which would mix two voices in what should read as one text). A
     * handful of individual canto numbers (e.g. 55-58 in Yuddha Kanda) are
     * also absent from Griffith's own numbering, not lost in extraction —
     * he kept the traditional numbering for scholarly cross-reference
     * even where he omitted a passage as a probable later interpolation.
     */
    {
      id: 'ramayana',
      label: 'The Ramayana',
      tradition: 'Hinduism',
      license: 'Public domain — Ralph T.H. Griffith translation (1870-74)',
      urls: ['./ramayana.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Vishnu Purana — Manmatha Nath Dutt's translation (1894-96), based on
     * H.H. Wilson's earlier work, public domain. One row per numbered
     * Section (continuous prose, not verse-numbered), grouped under 6
     * traditional Books.
     */
    {
      id: 'vishnu-purana',
      label: 'Vishnu Purana',
      tradition: 'Hinduism',
      license: 'Public domain — Manmatha Nath Dutt translation (1894-96), after H.H. Wilson',
      urls: ['./vishnu-purana.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Manusmriti ("The Laws of Manu") — George Bühler's 1886 translation
     * (Sacred Books of the East vol. 25), public domain, complete, all 12
     * chapters. Verse 6:76-77 is preserved as a single combined row because
     * Bühler's own translation merges those two verses into one continuous
     * rendering — not a gap in extraction.
     */
    {
      id: 'manusmriti',
      label: 'Manusmriti (Laws of Manu)',
      tradition: 'Hinduism',
      license: 'Public domain — George Bühler translation (1886), Sacred Books of the East vol. 25',
      urls: ['./manusmriti.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    {
      id: 'dhammapada',
      label: 'Dhammapada',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      /*
       * Fetches the plain-English translation files directly from SuttaCentral's
       * own bilara-data repo on GitHub (published branch), instead of their
       * `/api/bilarasuttas/...` endpoint. That API returns several parallel
       * segment maps for the same sutta in one payload — root-language (Pali,
       * itself in Latin transliteration so it can't be told apart from English
       * by script alone), an HTML-templated variant, and the plain translation —
       * with no reliable way to tell which map is which from content alone,
       * which is what caused HTML/placeholder markup to show up as "code"
       * instead of readable text. These files contain only the English
       * translation, so there's nothing left to disambiguate.
       */
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/dhp';
        const VAGGAS = [
          'dhp1-20','dhp21-32','dhp33-43','dhp44-59','dhp60-75','dhp76-89','dhp90-99',
          'dhp100-115','dhp116-128','dhp129-145','dhp146-156','dhp157-166','dhp167-178',
          'dhp179-196','dhp197-208','dhp209-220','dhp221-234','dhp235-255','dhp256-272',
          'dhp273-289','dhp290-305','dhp306-319','dhp320-333','dhp334-359','dhp360-382',
          'dhp383-423'
        ];
        const chunks = await Promise.all(VAGGAS.map(async (uid) => {
          try {
            const res = await fetch(`${BASE}/${uid}_translation-en-sujato.json`);
            if (!res.ok) return [];
            const data = await res.json();
            const byVerse = {};
            Object.entries(data).forEach(([segId, text]) => {
              const m = segId.match(/^(dhp\d+)[:.](\d+)/i);
              if (!m) return;
              // Segments numbered 0.x are chapter headings/titles, not verse content.
              if (m[2] === '0') return;
              const clean = stripHtml(String(text)).trim();
              if (!clean) return;
              const key = m[1].toLowerCase();
              (byVerse[key] = byVerse[key] || []).push(clean);
            });
            return Object.entries(byVerse).map(([key, lines]) => ({
              ref: `Dhammapada ${key.replace(/^dhp/i, '')}`,
              text: lines.join(' ').replace(/\s+/g, ' ').trim()
            })).filter(r => r.text.length > 0);
          } catch (err) {
            return [];
          }
        }));
        return chunks.flat();
      }
    },
    /*
     * Digha Nikaya (Long Discourses) and Majjhima Nikaya (Middle Discourses) —
     * the two most-cited collections in the Pali Canon's Sutta Pitaka, same
     * translator/license/repo as Dhammapada above. Both use a clean numeric
     * range with no gaps (verified against the repo's file listing), so they
     * share the fetchSujatoNikaya() routine defined near stripHtml() instead
     * of needing Dhammapada's hardcoded chunk list.
     */
    {
      id: 'digha-nikaya',
      label: 'Digha Nikaya (Long Discourses)',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: () => fetchSujatoNikaya('dn', 34, 'Digha Nikaya')
    },
    {
      id: 'majjhima-nikaya',
      label: 'Majjhima Nikaya (Middle Discourses)',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: () => fetchSujatoNikaya('mn', 152, 'Majjhima Nikaya')
    },
    /*
     * Sutta Nipata — one of the oldest strata of the Pali Canon (Khuddaka
     * Nikaya), 5 vaggas with a clean per-vagga file count (verified against
     * the repo's file listing: 12,14,12,16,19). Same translator/license/repo.
     */
    {
      id: 'sutta-nipata',
      label: 'Sutta Nipata',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/snp';
        const VAGGA_COUNTS = [12, 14, 12, 16, 19];
        const targets = [];
        VAGGA_COUNTS.forEach((count, vi) => {
          for (let n = 1; n <= count; n++) targets.push({ vagga: vi + 1, n });
        });
        const rows = await Promise.all(targets.map(async ({ vagga, n }) => {
          const url = `${BASE}/vagga${vagga}/snp${vagga}.${n}_translation-en-sujato.json`;
          const text = await fetchSujatoVerse(url);
          return text ? { ref: `Sutta Nipata ${vagga}.${n}`, text } : null;
        }));
        return rows.filter(Boolean);
      }
    },
    /*
     * Udana ("Heartfelt Sayings") — Khuddaka Nikaya, 8 vaggas of exactly 10
     * suttas each (verified against the repo's file listing).
     */
    {
      id: 'udana',
      label: 'Udana',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/ud';
        const targets = [];
        for (let vagga = 1; vagga <= 8; vagga++) {
          for (let n = 1; n <= 10; n++) targets.push({ vagga, n });
        }
        const rows = await Promise.all(targets.map(async ({ vagga, n }) => {
          const url = `${BASE}/vagga${vagga}/ud${vagga}.${n}_translation-en-sujato.json`;
          const text = await fetchSujatoVerse(url);
          return text ? { ref: `Udana ${vagga}.${n}`, text } : null;
        }));
        return rows.filter(Boolean);
      }
    },
    /*
     * Itivuttaka ("This Was Said") — Khuddaka Nikaya, 112 short suttas with
     * flat global numbering (iti1-iti112) but stored across 11 vagga folders;
     * ITI_VAGGA_BOUNDS below is the upper sutta number of each vagga,
     * verified against the repo's file listing.
     */
    {
      id: 'itivuttaka',
      label: 'Itivuttaka',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/iti';
        const ITI_VAGGA_BOUNDS = [10, 20, 27, 37, 49, 59, 69, 79, 89, 99, 112];
        const vaggaFor = (num) => {
          for (let v = 0; v < ITI_VAGGA_BOUNDS.length; v++) {
            if (num <= ITI_VAGGA_BOUNDS[v]) return v + 1;
          }
          return ITI_VAGGA_BOUNDS.length;
        };
        const rows = await Promise.all(Array.from({ length: 112 }, (_, i) => i + 1).map(async (num) => {
          const vagga = vaggaFor(num);
          const url = `${BASE}/vagga${vagga}/iti${num}_translation-en-sujato.json`;
          const text = await fetchSujatoVerse(url);
          return text ? { ref: `Itivuttaka ${num}`, text } : null;
        }));
        return rows.filter(Boolean);
      }
    },
    /*
     * Selected Suttas — a hand-picked set of the single most iconic suttas
     * from the Samyutta and Anguttara Nikayas: the first sermon (Four Noble
     * Truths, Middle Way), the second sermon (non-self), the Fire Sermon
     * (third major sermon), dependent origination and the noble eightfold
     * path formally defined, the "leaves in the hand" simile on the limited
     * scope of what the Buddha taught, and the Kalama Sutta on not accepting
     * claims on authority alone. These two Nikayas together hold thousands of
     * suttas split across ~3,200 small, irregularly-ranged files (an1-an11,
     * sn1-sn56 each subdivided further) — fetching the full corpus the way
     * Digha/Majjhima are above was tried and rejected as impractical (way
     * more requests than the rest of this app's sources combined, and no
     * clean sequential numbering to generate the fetch list from). This list
     * covers what a reader is actually likely to look for from these two
     * collections without that cost.
     */
    {
      id: 'sn-an-highlights',
      label: 'Selected Suttas (Samyutta & Anguttara Nikayas)',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const results = await Promise.all(SN_AN_HIGHLIGHTS.map(t => fetchSujatoSutta(t.path, t.prefix, t.label)));
        return results.flat();
      }
    },
    /*
     * Heart Sutra — both recensions (Larger, with its narrative frame, and
     * Smaller, the version normally chanted), from Max Muller's 1894
     * translation (Sacred Books of the East vol. 49). Hand-corrected against
     * the archive.org OCR scan (buddhistmahy02cowe): the underlying prose is
     * clean, but Muller's Sanskrit transliteration diacritics (e.g.
     * "Pra^;7aparamita", "^'ariputra") don't survive OCR at all, so proper
     * nouns and technical terms were manually restored to plain spelling
     * (Prajnaparamita, Shariputra, Bhagavat, Nirvana, etc.) — this is the
     * one source this session that's typed by hand rather than scripted,
     * since at ~700 words total it was faster and more reliable than writing
     * a cleanup pass for a two-paragraph text.
     */
    {
      id: 'heart-sutra',
      label: 'Heart Sutra',
      tradition: 'Buddhism',
      license: 'Public domain — F. Max Muller translation (1894), Sacred Books of the East vol. 49',
      urls: ['./heart-sutra.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The six canonical Sunni hadith collections (Kutub al-Sittah), from the
     * same author/project as the Qur'an source above (fawazahmed0), same
     * Unlicense terms. Each collection is one large JSON file — verified
     * shape: { metadata, hadiths: [{ hadithnumber, text, reference: {book,
     * hadith} }] } — so no discovery or per-chunk fetching is needed, unlike
     * Dhammapada above.
     */
    {
      id: 'hadith-bukhari',
      label: 'Sahih al-Bukhari',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-bukhari.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sahih al-Bukhari ${ref.book}:${ref.hadith}`
          : `Sahih al-Bukhari #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-muslim',
      label: 'Sahih Muslim',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-muslim.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sahih Muslim ${ref.book}:${ref.hadith}`
          : `Sahih Muslim #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-abudawud',
      label: 'Sunan Abu Dawud',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-abudawud.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sunan Abu Dawud ${ref.book}:${ref.hadith}`
          : `Sunan Abu Dawud #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-tirmidhi',
      label: 'Jami at-Tirmidhi',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-tirmidhi.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Jami at-Tirmidhi ${ref.book}:${ref.hadith}`
          : `Jami at-Tirmidhi #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-nasai',
      label: "Sunan an-Nasa'i",
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-nasai.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sunan an-Nasa'i ${ref.book}:${ref.hadith}`
          : `Sunan an-Nasa'i #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-ibnmajah',
      label: 'Sunan Ibn Majah',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-ibnmajah.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sunan Ibn Majah ${ref.book}:${ref.hadith}`
          : `Sunan Ibn Majah #${row.hadithnumber}`;
      })
    },
    /*
     * Vendored locally (same reason as fuse.min.js — no dependency on a
     * third party staying up) rather than fetched cross-origin.
     * baltimore-catechism.json is a cleaned Q/A extraction of "An
     * Explanation of the Baltimore Catechism of Christian Doctrine" (Rev.
     * Thomas L. Kinkead, "Baltimore Catechism No. 4", imprimatur 1891/1921)
     * from Project Gutenberg ebook #14554 — confirmed public domain, unlike
     * the modern Catechism of the Catholic Church (1992), which remains
     * under copyright. It predates a few dogmas defined after 1921 (e.g.
     * the Assumption, 1950), so it's an older but still authoritative and
     * doctrinally representative Catholic catechism, not the current one.
     */
    {
      id: 'baltimore-catechism',
      label: 'Baltimore Catechism',
      tradition: 'Catholicism',
      license: 'Public domain — Project Gutenberg ebook #14554',
      urls: ['./baltimore-catechism.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({
            ref: `Baltimore Catechism, Q. ${row.n}`,
            text: stripHtml(`Q. ${row.q} A. ${row.a}`)
          }))
        : []
    },
    /*
     * The seven deuterocanonical Old Testament books, a.k.a. the Apocrypha —
     * kept as their own tradition rather than folded into "Catholicism",
     * since they're not a distinctively Catholic composition: the Orthodox
     * churches also hold them as canonical, and they were printed in early
     * Protestant Bibles (including the original 1611 KJV) as a separate
     * section, before falling out of common Protestant use. Douay-Rheims
     * (Challoner revision, mid-18th century) is the traditional English
     * Catholic translation and is public domain (pre-1923).
     *
     * Vendored locally rather than fetched from a third-party GitHub JSON
     * (xxruyle/Bible-DouayRheims) after that source turned out to have a
     * real data bug: its "2 Machabees" is missing chapter 7 entirely (the
     * martyrdom of the seven brothers), silently shifting every later verse
     * back by one chapter under the wrong number. deuterocanon.json here was
     * extracted directly from Project Gutenberg ebook #1581 instead — its
     * chapter counts for all seven books were verified against the standard
     * Douay-Rheims structure (no gaps), and Challoner's inline footnotes
     * (appended to verse text as "...."-delimited asides in that edition)
     * were stripped so only the actual verse text remains.
     */
    {
      id: 'deuterocanon',
      label: 'Apocrypha / Deuterocanon (Douay-Rheims)',
      tradition: 'Apocrypha',
      license: 'Public domain (pre-1923) — Douay-Rheims, Challoner revision, via Project Gutenberg ebook #1581',
      urls: ['./deuterocanon.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The 126 dogmatic canons of the Council of Trent (1545-1563) — the
     * "if any one saith X, let him be anathema" statements on Justification,
     * the Sacraments, the Eucharist, Penance, Order, Matrimony, etc. Public
     * domain (J. Waterworth's 1848 translation). Vendored from a single-page
     * transcription hosted by the Hanover Historical Texts Project rather
     * than fetched live — it's plain HTML on an institutional site with no
     * CORS headers for cross-origin fetch, and no JSON version exists.
     *
     * That transcription has a handful of real typos (e.g. "CANON lI." and
     * "CANON 11." for "CANON II.", "let him be be anathema" for "let him be
     * anathema") which were corrected during extraction — verified by
     * checking the canon count for every session/topic against the
     * unmodified source (all 126 accounted for, none merged or dropped).
     *
     * Decrees (as opposed to canons) are not included: Trent's canons are
     * short, numbered, and self-contained ("if anyone says X, anathema"),
     * which is what makes them directly comparable entries; the decrees are
     * long discursive prose better suited to reading in full than to
     * verse-style search results.
     */
    {
      id: 'trent-canons',
      label: 'Council of Trent — Canons',
      tradition: 'Catholicism',
      license: 'Public domain — J. Waterworth translation (1848), via Hanover Historical Texts Project',
      urls: ['./trent-canons.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Two pre-1929 papal encyclicals, both firmly public domain by age —
     * Rerum Novarum (Leo XIII, 1891, the foundational document of Catholic
     * social teaching) and Ineffabilis Deus (Pius IX, 1854, the dogmatic
     * definition of the Immaculate Conception, already cited in the CCC-era
     * entries above via the Baltimore Catechism's summary of it). Vendored
     * from papalencyclicals.net rather than fetched live (no CORS headers,
     * and no JSON version exists). That site doesn't number every paragraph
     * consistently across the whole document, so paragraphs are numbered
     * sequentially here rather than reusing the site's own inline numbers —
     * a stable, collision-free citation was judged more important than
     * matching another site's paragraph numbering exactly.
     */
    {
      id: 'rerum-novarum',
      label: 'Rerum Novarum (Leo XIII, 1891)',
      tradition: 'Catholicism',
      license: 'Public domain (pre-1929)',
      urls: ['./rerum-novarum.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    {
      id: 'ineffabilis-deus',
      label: 'Ineffabilis Deus (Pius IX, 1854)',
      tradition: 'Catholicism',
      license: 'Public domain (pre-1929)',
      urls: ['./ineffabilis-deus.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Kybalion (1908), attributed to "Three Initiates" — the
     * foundational modern text of Hermeticism, laying out the Seven
     * Hermetic Principles (Mentalism, Correspondence, Vibration, Polarity,
     * Rhythm, Cause and Effect, Gender). Public domain (Project Gutenberg
     * ebook #14209). Vendored locally as a flat paragraph-by-paragraph
     * extraction, one row per prose paragraph within each of its 15
     * chapters — mirroring the Rerum Novarum/Ineffabilis Deus "para. N"
     * citation style above, since this too is discursive prose rather than
     * verse-numbered scripture.
     *
     * Filed under Gnosticism rather than a standalone "Hermeticism"
     * category (2026-07-27 restructure) — Hermeticism was folded in here
     * alongside Corpus Hermeticum and the Emerald Tablet as one broader
     * Western-esoteric/gnostic-adjacent grouping, at the user's request.
     */
    {
      id: 'kybalion',
      label: 'The Kybalion',
      tradition: 'Gnosticism',
      license: 'Public domain — Project Gutenberg ebook #14209',
      urls: ['./kybalion.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Emerald Tablet (Tabula Smaragdina) — a short, single-paragraph
     * Hermetic text (source of "as above, so below") with a real
     * manuscript history: attested in Arabic alchemical sources from
     * roughly the 8th-9th century, translated into Latin by the 12th
     * century, and hugely influential on Western alchemy since. Its
     * attribution to Hermes Trismegistus is legendary, like the rest of
     * the Hermetica, but the text itself is a genuine, long-transmitted
     * document — unlike the modern (1930s, channeled, unverifiable)
     * "Emerald Tablets of Thoth" attributed to Maurice Doreal, which is
     * not included here for that reason.
     *
     * Text is Isaac Newton's own English translation, c. 1680 (Newton
     * personally studied and copied Hermetic/alchemical texts) — safely
     * public domain by age regardless of which modern edition transcribes
     * it. Spelling has been lightly modernized from Newton's original
     * manuscript shorthand ("wch" -> "which", "ye" -> "the", etc.) for
     * readability; wording is otherwise unchanged. The original numbering
     * has a few lettered sub-verses (6a, 7a, 11a) which are renumbered
     * sequentially here, the same citation-stability tradeoff already
     * made for Rerum Novarum/Ineffabilis Deus above.
     *
     * Filed under Gnosticism (see Kybalion note above) rather than a
     * standalone Hermeticism category.
     */
    {
      id: 'emerald-tablet',
      label: 'The Emerald Tablet',
      tradition: 'Gnosticism',
      license: 'Public domain — Isaac Newton\'s translation, c. 1680',
      urls: ['./emerald-tablet.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Yasna, the core liturgy of the Avesta, translated by L. H. Mills
     * (Sacred Books of the East vol. 31, American Edition 1898) — public
     * domain by age and by translator's death (1918). Vendored locally as
     * one row per verse, ref format "Yasna N.M".
     *
     * Deliberately excludes three parts of the 72-chapter Yasna that a
     * commonly-cited modern compilation (sacred-texts.com) swaps a
     * different, unclear-license translation into: the five actual
     * Gathas (Yasna 28-34, 43-51, 53 — rendered there via Bartholomae/
     * Taraporewala, "The Divine Songs of Zarathushtra", 1951, likely still
     * under copyright) and the Zoroastrian Creed (Yasna 12 — rendered
     * there via a 1996 J. H. Peterson translation of unclear license).
     * Everything else (Yasna 0-11, 13-27, 35-42, 52, 54-72) is Mills' own
     * clearly public-domain 1898 wording, confirmed chapter-by-chapter
     * against the translator attributions on that page — still a
     * substantial corpus including the Yasna Haptanghaiti ("Seven
     * Chapters", linguistically as old as the Gathas), the Fravashi
     * invocations, the Fire hymns, and the Ahunwar/Ashem Vohu commentary.
     */
    {
      id: 'yasna',
      label: 'The Yasna',
      tradition: 'Zoroastrianism',
      license: 'Public domain — L. H. Mills translation, Sacred Books of the East vol. 31 (American Edition, 1898)',
      urls: ['./yasna.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Key to Theosophy (1889) by H. P. Blavatsky — a Q&A-format
     * primer on the Theosophical Society's core doctrines. Public domain
     * (Project Gutenberg ebook #55618); Blavatsky died 1891. Vendored as
     * one row per Q/A turn or paragraph, ref "The Key to Theosophy,
     * Section N, para. M".
     */
    {
      id: 'key-to-theosophy',
      label: 'The Key to Theosophy',
      tradition: 'Theosophy',
      license: 'Public domain — Project Gutenberg ebook #55618',
      urls: ['./key-to-theosophy.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Secret Doctrine (1888) by H. P. Blavatsky — her magnum opus,
     * Vol. I "Cosmogenesis" and Vol. II "Anthropogenesis". Public domain
     * (Project Gutenberg ebooks #54824 and #54488). Vendored as one row
     * per paragraph, grouped by Proem/Part I/II/III, ref "The Secret
     * Doctrine, Cosmogenesis|Anthropogenesis, <Part>, para. N".
     *
     * Deliberately limited to the original two volumes (Cosmogenesis +
     * Anthropogenesis), matching the 1888 first edition. Project
     * Gutenberg additionally hosts a "Vol. 3 of 4" (a separate 1897
     * posthumous compilation of Blavatsky's other papers, edited by
     * Besant/Mead — not part of the original work) and a "Vol. 4 of 4"
     * (a pure alphabetical index to the other three, no independent
     * content) — both skipped as out of scope for a source-text corpus.
     */
    {
      id: 'secret-doctrine',
      label: 'The Secret Doctrine',
      tradition: 'Theosophy',
      license: 'Public domain — Project Gutenberg ebooks #54824 (Vol. I) and #54488 (Vol. II)',
      urls: ['./secret-doctrine.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Book of the Law (Liber AL vel Legis), the founding scripture of
     * Thelema — dictated to Aleister Crowley in 1904, first published
     * 1909 (privately, by the A∴A∴, as part of "ΘΕΛΗΜΑ"), safely public
     * domain under the pre-1923 bright-line rule regardless of any
     * copyright O.T.O. asserts over Crowley's later works.
     *
     * Deliberately does NOT include a longer Crowley text (e.g. Magick in
     * Theory and Practice, requested as a second, bigger source). The
     * only available transcription found (sacred-texts.com, sourced from
     * an O.T.O.-distributed diskette edition) carries an explicit
     * "Copyright (c) O.T.O." notice with a restrictive "personal use or
     * research" license — incompatible with vendoring it into a public,
     * freely-redistributable app corpus, regardless of the 1929
     * first-edition text's own age. Project Gutenberg, notably, hosts two
     * of Crowley's secular literary works but none of his occult corpus,
     * consistent with that being genuinely unclear/actively-claimed
     * territory rather than settled public domain.
     */
    {
      id: 'book-of-the-law',
      label: 'The Book of the Law',
      tradition: 'Thelema',
      license: 'Public domain (first published 1909, pre-1923)',
      urls: ['./book-of-the-law.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Yoruba Religion — the West African tradition of Orisha veneration
     * that Santería (Cuba) and related New World traditions (Vodou,
     * Candomblé) descend from. Text is A. B. Ellis, "The Yoruba-Speaking
     * Peoples of the Slave Coast of West Africa" (1894), a British
     * colonial-era ethnography — public domain (archive.org marks it
     * NOT_IN_COPYRIGHT; pre-1923). Kept as its own tradition rather than
     * relabeled "Santería": it documents the 19th-century ancestral
     * African religion, not the distinct Cuban Catholic-syncretic
     * tradition, and it's an outsider's colonial-era account rather than
     * the tradition's own voice — the same caveat already applied to the
     * Skull and Bones entry's 1876 source.
     *
     * Vendored from an OCR scan (archive.org djvu text), not a clean
     * proofread transcription — no equivalent exists elsewhere. Obvious
     * scan-noise fragments (stray characters, illustration captions) were
     * filtered out algorithmically, but some letter-level OCR noise
     * remains in the running text (this is a lower fidelity source than
     * the Gutenberg/Wikisource-sourced texts elsewhere in this file).
     * Limited to the religion-focused chapters (Chief Gods, Minor Gods,
     * Priests and Worship, Egungun/superstitions, Indwelling Spirits,
     * birth/marriage/death ceremonies, and folk-lore tales) — chapters on
     * language, law, government, and proverbs were excluded as out of
     * scope for a source-text corpus.
     */
    {
      id: 'yoruba-religion',
      label: 'Yoruba Religion (Ellis, 1894)',
      tradition: 'Yoruba Religion',
      license: 'Public domain — A. B. Ellis, 1894 (archive.org, not in copyright)',
      urls: ['./yoruba-religion.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Gnosticism — Pistis Sophia, the single most substantial Gnostic
     * scripture to survive antiquity (a Coptic manuscript bought by the
     * British Museum in 1785). Unlike the Nag Hammadi texts (Gospel of
     * Thomas, Apocryphon of John, etc. — not discovered until 1945, so no
     * translation of them is old enough to be public domain), Pistis
     * Sophia was already known and translated well before 1923. Used here:
     * George Horner's 1924 literal translation (Society for Promoting
     * Christian Knowledge), via Project Gutenberg ebook #76266 — a clean,
     * proofread transcription, not an OCR scan, and safely public domain
     * both by original 1924 publication date and by Gutenberg's own
     * clearance process. Trimmed to just Horner's translation of the five
     * Documents themselves (`pistis-sophia.json`, 202 paragraph rows, ref
     * `Pistis Sophia (Horner, 1924), <Document>, para. N`) — Francis
     * Legge's lengthy scholarly introduction and the back-matter indices
     * are excluded as out of scope, the same principle already applied to
     * Trent's canons and the Summa's Objections/Replies.
     */
    {
      id: 'pistis-sophia',
      label: 'Pistis Sophia (Horner, 1924)',
      tradition: 'Gnosticism',
      license: 'Public domain — George Horner, trans., Pistis Sophia (1924); Project Gutenberg ebook #76266',
      urls: ['./pistis-sophia.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Corpus Hermeticum — the 13 core Hermetic treatises (Poemandres
     * through The Secret Sermon on the Mountain), G.R.S. Mead's 1906
     * translation ("Thrice-Greatest Hermes"), public domain. Vendored from
     * an archive.org PDFy mirror of the plain-text edition (clean,
     * paragraph/section text, not an OCR scan). Ref format
     * `Corpus Hermeticum (Mead, 1906), Libellus <roman>, <section>`,
     * matching the standard modern citation convention for this text
     * (e.g. "CH I.4"). 191 numbered-section rows. Filed under Gnosticism
     * alongside the Kybalion/Emerald Tablet (2026-07-27 restructure) rather
     * than a standalone Hermeticism category — see the Kybalion entry
     * above for why.
     */
    {
      id: 'corpus-hermeticum',
      label: 'Corpus Hermeticum (Mead, 1906)',
      tradition: 'Gnosticism',
      license: 'Public domain — G.R.S. Mead, trans., The Corpus Hermeticum (1906)',
      urls: ['./corpus-hermeticum.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * "Setna and the Magic Book" — an ancient Egyptian tale (Ptolemaic-era
     * Demotic, ~1st century BC/AD) about Prince Setna Khaemwaset's quest
     * for the legendary Book of Thoth. This is the closest thing to a
     * public-domain "Book of Thoth" text that actually exists: no
     * translation of any real ancient "Book of Thoth" content is public
     * domain (see below), so this is the tale ABOUT the book — a story
     * about finding and reading it — rather than the book's own purported
     * words. Included at the user's explicit request as a stand-in for
     * that gap, with this caveat carried in the label/cards.
     *
     * Translation: W. M. Flinders Petrie, "Egyptian Tales, Translated from
     * the Papyri: Second Series" (1895), public domain, Project Gutenberg
     * ebook #7413. Trimmed to just the tale itself (33 paragraph rows, ref
     * `Setna and the Magic Book (Petrie, 1895), para. N`) — Petrie's
     * scholarly "Remarks" section following it is excluded as out of
     * scope, same principle as Pistis Sophia's introduction.
     *
     * Not usable: Richard Jasnow & Karl-Theodor Zauzich's modern scholarly
     * reconstruction of actual Demotic "Book of Thoth" fragments
     * (originally published 2005, revised as "Conversations in the House
     * of Life," Harrassowitz Verlag, 2014) — still under active copyright,
     * confirmed still commercially sold. Also not usable: Aleister
     * Crowley's "The Book of Thoth" (1944) — an unrelated Tarot text, not
     * ancient, and under the same O.T.O.-defended copyright that already
     * ruled out a longer Crowley text for Thelema.
     */
    {
      id: 'setna-magic-book',
      label: 'Setna and the Magic Book (Petrie, 1895)',
      tradition: 'Gnosticism',
      license: 'Public domain — W. M. Flinders Petrie, trans., Egyptian Tales, Second Series (1895); Project Gutenberg ebook #7413',
      urls: ['./setna-magic-book.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Egyptian Occultism — a new sidebar category (2026-07-27) distinct
     * from Gnosticism, covering ancient Egyptian funerary/magical texts and
     * the Victorian Egyptological scholarship that first translated them.
     * Five texts:
     *
     * 1. The Pyramid Texts — the oldest religious texts in the world (Old
     * Kingdom, ~2400-2300 BC), carved into pyramid walls at Saqqara.
     * Samuel A. B. Mercer's 1952 translation was the first-ever complete
     * translation in any language. Public domain in the US: registered
     * 1952, would have required renewal in 1980, and no renewal was found
     * in the US Copyright Office database (per sacred-texts.com's own
     * copyright research, which is where this was vendored from via a
     * Wayback Machine mirror — the live site is behind a Cloudflare
     * bot-challenge that blocks curl/WebFetch). `pyramid-texts.json`, 700
     * of 714 Utterances (a few numbering gaps in the source pages), ref
     * `The Pyramid Texts (Mercer, 1952), Utterance N`.
     */
    {
      id: 'pyramid-texts',
      label: 'The Pyramid Texts (Mercer, 1952)',
      tradition: 'Egyptian Occultism',
      license: 'Public domain — Samuel A. B. Mercer, trans. (1952); copyright not renewed',
      urls: ['./pyramid-texts.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * 2. The Book of the Dead — E. A. Wallis Budge's 1895 translation of
     * the Papyrus of Ani (British Museum), the most famous single Book of
     * the Dead manuscript. Public domain, vendored from a clean HTML
     * transcription (sacred-texts.com, via Wayback Machine mirror — same
     * Cloudflare situation as above) rather than an OCR scan. Limited to
     * the actual translated Plates (I-XXXVII, the spells/chapters
     * themselves); Budge's lengthy introductory essays (Legend of Osiris,
     * Doctrine of Eternal Life, etc.) are excluded as out of scope, the
     * same principle applied to Pistis Sophia's introduction. `book-of-
     * the-dead.json`, 121 rows, ref `The Book of the Dead (Budge, 1895),
     * Plate <roman>[, Ch. N | , para. N]`.
     */
    {
      id: 'book-of-the-dead',
      label: 'The Book of the Dead (Budge, 1895)',
      tradition: 'Egyptian Occultism',
      license: 'Public domain — E. A. Wallis Budge, trans., The Papyrus of Ani (1895)',
      urls: ['./book-of-the-dead.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * 3. The Demotic Magical Papyrus of London and Leiden — a 2nd/3rd-
     * century AD Egyptian magical handbook (divination, healing, and
     * binding spells), edited and translated by F. Ll. Griffith and
     * Herbert Thompson (1904-1909), public domain. Vendored from a clean
     * HTML transcription (sacred-texts.com via Wayback Machine). One row
     * per papyrus column (recto Col. I-XXIX, verso Col. I-XXXIII),
     * `demotic-magical-papyrus.json`, 62 rows, ref `The Demotic Magical
     * Papyrus of London and Leiden (Griffith & Thompson, 1904-1909),
     * [Verso] Col. <roman>`.
     */
    {
      id: 'demotic-magical-papyrus',
      label: 'Demotic Magical Papyrus of London and Leiden',
      tradition: 'Egyptian Occultism',
      license: 'Public domain — F. Ll. Griffith & Herbert Thompson, eds./trans. (1904-1909)',
      urls: ['./demotic-magical-papyrus.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * 4. The Greek Magical Papyri (PGM) — the modern comprehensive
     * collection (Betz, 1986) was literally the first-ever complete
     * English translation, so no full PGM corpus is public domain. What
     * IS public domain: Charles Wycliffe Goodwin's 1852 translation of one
     * specific papyrus (British Museum Papyrus XLVI Greek, part of the
     * Anastasi collection) — edited for the Cambridge Antiquarian Society,
     * "Fragment of a Graeco-Egyptian Work upon Magic." This represents ONE
     * papyrus, not the full PGM corpus — labeled accordingly everywhere it
     * appears. Vendored from an archive.org OCR scan of the original 1852
     * Cambridge edition, which interleaves each spell's untranslated Greek
     * original with Goodwin's English translation; only the English
     * translation halves were kept (the Greek OCR'd as unreadable
     * transliteration noise) and hand-verified, 7 of the ~17 original
     * spells recovered cleanly enough to vendor. `goodwin-pgm.json`, ref
     * `Fragment of a Graeco-Egyptian Work upon Magic (Goodwin, 1852),
     * Spell N`.
     */
    {
      id: 'goodwin-pgm',
      label: 'Graeco-Egyptian Magic Papyrus (Goodwin, 1852)',
      tradition: 'Egyptian Occultism',
      license: 'Public domain — Charles Wycliffe Goodwin, trans. (1852); one papyrus, not the full PGM corpus',
      urls: ['./goodwin-pgm.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * 5. Egyptian Magic — E. A. Wallis Budge's 1899 survey of Egyptian
     * magical practice (amulets, names of power, magical figures,
     * ceremonies, and the worship of animals), public domain. Vendored
     * from an archive.org OCR scan (Google Books mirror) — noisier than
     * the Gutenberg/sacred-texts-HTML sources above, comparable to the
     * Yoruba Religion/Vodou tier; residual OCR noise and interleaved
     * footnotes remain in the running text. Chapter headings did not OCR
     * cleanly enough to recover reliably, so this is vendored as a flat
     * paragraph sequence rather than chapter-grouped. `egyptian-magic
     * .json`, 425 rows, ref `Egyptian Magic (Budge, 1899), para. N`.
     */
    {
      id: 'egyptian-magic',
      label: 'Egyptian Magic (Budge, 1899)',
      tradition: 'Egyptian Occultism',
      license: 'Public domain — E. A. Wallis Budge (1899)',
      urls: ['./egyptian-magic.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Vodou (Haitian) — unlike Yoruba Religion or Santería, no reliable
     * public-domain source text exists for this tradition. Every pre-1930
     * PD English account (Spenser St. John 1884, Hesketh Prichard 1900,
     * this book) is considered sensationalized/fabricated by scholarly
     * consensus — St. John's is literally the origin of the "voodoo
     * cannibalism" myth — and the trustworthy 20th-century corrective
     * accounts (Herskovits 1937, Hurston's Tell My Horse 1938) are both
     * still under copyright (Hurston confirmed via renewal record through
     * 2034; Herskovits still actively in print via a licensed reprint
     * house). Seabrook's The Magic Island (1929) is used here as the only
     * available PD option, curated: limited to Part One, "The Voodoo
     * Rites" (Foreword through Ch. VI, "The God Incarnate") — the
     * first-person, descriptive-ceremony chapters — while explicitly
     * excluding all of Part Two, "Black Sorcery" (the zombie-mythology
     * chapters that are this book's most notorious fabrications). Even
     * within the kept chapters, this is a sensationalized white outsider's
     * 1929 travel memoir, not the tradition's own voice and not verified
     * doctrine — treat every quote from it as "according to this account,"
     * the same caveat already applied to Yoruba Religion and Skull and
     * Bones. Vendored from an OCR scan (archive.org djvu text); noisier
     * than most sources here, with residual line-hyphenation and
     * character-level OCR noise, and illustration-caption fragments
     * filtered out algorithmically on a best-effort basis.
     */
    {
      id: 'vodou',
      label: 'The Magic Island (Seabrook, 1929)',
      tradition: 'Vodou',
      license: 'Public domain — William Seabrook, The Magic Island (1929); a sensationalized outsider’s memoir, not an authoritative or verified Vodou source',
      urls: ['./vodoo.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Astrology — its own new top-level sidebar group ("Occultism"),
     * separate from Egyptian Occultism (which is specifically ancient
     * Egyptian funerary/magical texts). Source: Ptolemy's Tetrabiblos
     * ("Four Books"), the single most influential text in the history of
     * Western astrology — 2nd-century AD Alexandria, still the foundation
     * of natal/horoscopic astrology today. J. M. Ashmand's 1822 English
     * translation, safely public domain (pre-1923 bright-line rule),
     * vendored directly from Project Gutenberg ebook #70850 (a clean HTML
     * transcription, not an OCR scan). `tetrabiblos.json`, 480 rows across
     * all 70 chapters of Books I-IV, ref `Ptolemy's Tetrabiblos (Ashmand,
     * 1822), Book <roman>, Ch. N, para. M`. Excludes the front-matter
     * Preface and the back-matter Appendix (extracts from Ptolemy's
     * separate Almagest, not the Tetrabiblos itself) — same in/out-of-scope
     * principle applied to Pistis Sophia's introduction and Setna's
     * "Remarks".
     */
    {
      id: 'tetrabiblos',
      label: "Ptolemy's Tetrabiblos (Ashmand, 1822)",
      tradition: 'Astrology',
      license: 'Public domain — J. M. Ashmand, trans., Ptolemy’s Tetrabiblos (1822)',
      urls: ['./tetrabiblos.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Summa Theologica (St. Thomas Aquinas, 13th c.) — the single most
     * influential work of Catholic systematic theology, and public domain
     * (Fathers of the English Dominican Province translation, 1920, via
     * Project Gutenberg ebooks #17611/17897/18755/19950 for Parts I, I-II,
     * II-II, and III respectively). 2,637 articles total.
     *
     * Each article's Objections and Replies are structured disputation, not
     * Aquinas's own position — omitted for the same reason Trent's decrees
     * were skipped (see above), keeping only the "I answer that..." section,
     * which is the article's actual teaching and is what gets cited in
     * practice.
     *
     * The source text itself has a handful of numbering slips (an article
     * bracket-tagged with the same number as its neighbor, or labeled with
     * the wrong Part letter) — verified by cross-checking against the
     * spelled-out ordinal ("FIRST ARTICLE", "SECOND ARTICLE", etc.), which
     * never skips or repeats. Article numbers here are assigned by document
     * order within each Question rather than trusting either the bracket or
     * the ordinal word alone, since a few even had the ordinal word itself
     * duplicated. A handful of articles (5 out of 2,642 in Part I alone)
     * lack an extractable "I answer that" in this particular transcription
     * and are simply absent rather than guessed at.
     */
    {
      id: 'summa-theologica',
      label: 'Summa Theologica (Aquinas)',
      tradition: 'Catholicism',
      license: 'Public domain — Fathers of the English Dominican Province translation (1920), via Project Gutenberg',
      urls: ['./summa-theologica.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    }
  ];

  function stripHtml(str) {
    return str
      .replace(/<[^>]*>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/&quot;/gi, '"')
      .replace(/&#0*39;/gi, "'")
      .replace(/\s+/g, ' ')
      .trim();
  }

  /*
   * Shared fetch routine for whole-Nikaya SuttaCentral sources (Digha and
   * Majjhima) — each sutta is one file, numbered sequentially with no gaps,
   * so unlike Dhammapada above no hardcoded chunk list is needed. Segment ids
   * look like "dn1:1.1.1" (sutta:section.paragraph.sentence, depth varies by
   * sutta — some suttas add an extra top-level "chapter" number, e.g. long
   * suttas like DN 16). Rather than assume a fixed depth, every segment is
   * grouped with its siblings by dropping only the last (sentence-level)
   * component, then groups are numbered sequentially in file order — this
   * produces one row per paragraph regardless of how deeply that sutta happens
   * to be subdivided. Segments under "0" (nikaya/sutta title) are headings,
   * not body text, and are skipped — same convention as Dhammapada's "0.x".
   */
  // Splits one bilara segment map into ordered paragraph-level text blocks —
  // groups every segment by its id with the last (sentence-level) component
  // dropped, in first-appearance order. `idPrefix` is everything before the
  // colon in that file's segment ids (e.g. "dn1", or "sn56.11" for a Samyutta
  // sutta, whose own number already contains a dot).
  function groupSujatoSegments(data, idPrefix) {
    const escaped = idPrefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`^${escaped}:([\\d.]+)$`);
    const groupOrder = [];
    const groupLines = {};
    Object.keys(data).forEach((segId) => {
      const m = segId.match(re);
      if (!m) return;
      const parts = m[1].split('.');
      if (parts[0] === '0') return;
      const groupKey = parts.length > 1 ? parts.slice(0, -1).join('.') : parts[0];
      const clean = stripHtml(String(data[segId])).trim();
      if (!clean) return;
      if (!(groupKey in groupLines)) {
        groupLines[groupKey] = [];
        groupOrder.push(groupKey);
      }
      groupLines[groupKey].push(clean);
    });
    return groupOrder
      .map((key) => groupLines[key].join(' ').replace(/\s+/g, ' ').trim())
      .filter((text) => text.length > 0);
  }

  async function fetchSujatoNikaya(abbrev, count, label) {
    const BASE = `https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/${abbrev}`;
    const nums = Array.from({ length: count }, (_, i) => i + 1);
    const chunks = await Promise.all(nums.map(async (n) => {
      try {
        const res = await fetch(`${BASE}/${abbrev}${n}_translation-en-sujato.json`);
        if (!res.ok) return [];
        const data = await res.json();
        return groupSujatoSegments(data, `${abbrev}${n}`).map((text, i) => ({
          ref: `${label} ${n}.${i + 1}`,
          text
        }));
      } catch (err) {
        return [];
      }
    }));
    return chunks.flat();
  }

  // Fetches one specific SuttaCentral file at `path` (relative to the sujato
  // sutta directory) and splits it into paragraph rows via groupSujatoSegments,
  // for the curated Samyutta/Anguttara highlights list — those Nikayas are
  // chunked into thousands of small irregularly-ranged files (verified via
  // the repo's file tree), too many and too fragile to fetch a full range of
  // like Digha/Majjhima above, so only a hand-picked list of the single most
  // iconic suttas is fetched, each by its known exact file path.
  async function fetchSujatoSutta(path, idPrefix, label) {
    const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta';
    try {
      const res = await fetch(`${BASE}/${path}_translation-en-sujato.json`);
      if (!res.ok) return [];
      const data = await res.json();
      return groupSujatoSegments(data, idPrefix).map((text, i) => ({
        ref: `${label} ${i + 1}`,
        text
      }));
    } catch (err) {
      return [];
    }
  }

  /*
   * Fetches one SuttaCentral/Sujato file that is itself a single short sutta
   * (Sutta Nipata, Udana, Itivuttaka verses) rather than a long discourse —
   * unlike fetchSujatoNikaya above, there's no internal paragraph structure
   * worth splitting on, so every non-"0.x" segment in the file is just
   * concatenated into one row's text, the same way Dhammapada's verses work.
   */
  async function fetchSujatoVerse(url) {
    try {
      const res = await fetch(url);
      if (!res.ok) return null;
      const data = await res.json();
      const lines = [];
      Object.keys(data).forEach((segId) => {
        // segId's own prefix can itself contain a dot (e.g. "snp1.1:0.1"),
        // so split on the LAST colon rather than assuming a bare \w+ prefix.
        const m = segId.match(/:([\d.]+)$/);
        if (!m) return;
        if (m[1].split('.')[0] === '0') return;
        const clean = stripHtml(String(data[segId])).trim();
        if (clean) lines.push(clean);
      });
      const text = lines.join(' ').replace(/\s+/g, ' ').trim();
      return text || null;
    } catch (err) {
      return null;
    }
  }

  /*
   * Turns an arbitrary JSON payload into a flat [{ref, text}] list.
   * Handles: a bare array, a wrapper object ({quran: [...]}, {verses: [...]}, etc.),
   * and picks whichever text-bearing field actually exists on each row.
   */
  function normalizeVerseList(data, makeRef, opts) {
    const options = opts || {};
    let rows = null;

    if (Array.isArray(data)) {
      rows = data;
    } else if (data && typeof data === 'object') {
      // Find the first array-of-objects property in the payload.
      for (const val of Object.values(data)) {
        if (Array.isArray(val) && val.length && typeof val[0] === 'object') { rows = val; break; }
      }
    }
    if (!rows) return [];

    const TEXT_KEYS = ['translation', 'english', 'englishTranslation', 'et', 'text', 'description', 'meaning', 'content', 'verse_text'];

    // Is this string mostly Latin letters? Guards against indexing Devanagari /
    // Arabic script when we want the English translation field instead.
    const isMostlyLatin = (s) => {
      const letters = s.replace(/[^A-Za-z\u0080-\u024F\u0900-\u097F\u0600-\u06FF]/g, '');
      if (!letters.length) return false;
      const latin = (letters.match(/[A-Za-z]/g) || []).length;
      return latin / letters.length > 0.6;
    };

    return rows.map((row, i) => {
      if (!row || typeof row !== 'object') return null;
      let text = null;
      for (const k of TEXT_KEYS) {
        const val = row[k];
        if (typeof val === 'string' && val.trim().length > 0) {
          if (options.requireLatin && !isMostlyLatin(val)) continue;
          text = val.trim();
          break;
        }
      }
      if (!text) return null;
      return { ref: makeRef(row, i), text: stripHtml(text) };
    }).filter(Boolean);
  }

  const BIBLE_BOOKS = [
    "1Chronicles","1Corinthians","1John","1Kings","1Peter","1Samuel","1Thessalonians","1Timothy",
    "2Chronicles","2Corinthians","2John","2Kings","2Peter","2Samuel","2Thessalonians","2Timothy",
    "3John","Acts","Amos","Colossians","Daniel","Deuteronomy","Ecclesiastes","Ephesians","Esther",
    "Exodus","Ezekiel","Ezra","Galatians","Genesis","Habakkuk","Haggai","Hebrews","Hosea","Isaiah",
    "James","Jeremiah","Job","Joel","John","Jonah","Joshua","Jude","Judges","Lamentations","Leviticus",
    "Luke","Malachi","Mark","Matthew","Micah","Nahum","Nehemiah","Numbers","Obadiah","Philemon",
    "Philippians","Proverbs","Psalms","Revelation","Romans","Ruth","SongofSolomon","Titus","Zechariah","Zephaniah"
  ];

  const DB_NAME = 'tep-bible-data';
  const STORE = 'verses';

  function openBibleDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          db.createObjectStore(STORE, { keyPath: 'ref' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function getCachedVerseCount() {
    const db = await openBibleDB();
    return new Promise((resolve) => {
      const tx = db.transaction(STORE, 'readonly');
      const countReq = tx.objectStore(STORE).count();
      countReq.onsuccess = () => resolve(countReq.result);
      countReq.onerror = () => resolve(0);
    });
  }

  async function saveVersesToDB(verses) {
    const db = await openBibleDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, 'readwrite');
      const store = tx.objectStore(STORE);
      verses.forEach(v => store.put(v));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async function loadAllVersesFromDB() {
    const db = await openBibleDB();
    return new Promise((resolve) => {
      const tx = db.transaction(STORE, 'readonly');
      const req = tx.objectStore(STORE).getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => resolve([]);
    });
  }

  function setBibleStatus(mode, text) {
    const el = document.getElementById('bible-status');
    el.className = 'bible-status ' + mode;
    el.innerHTML = `<span class="dot"></span>${text}`;
  }

  // Bible is one indivisible index, so it lives in a single worker (index 0)
  // rather than being sharded — the other workers just never get bible data
  // and their bibleFuse stays null, which search-worker.js already handles.
  function loadBibleIntoWorkers(verses) {
    if (searchWorkers.length) searchWorkers[0].postMessage({ type: 'load', name: 'bible', data: verses });
  }

  // Splits source-text rows across the worker pool so every worker owns a
  // disjoint slice of the ~30 collections and searches its slice in
  // parallel with the others. A whole collection (e.g. all of the Qur'an,
  // or all of one Hadith book) always goes to a single worker together —
  // splitting one collection across workers would just mean re-merging its
  // own results, for no benefit. Collections vary hugely in size (the
  // Mahabharata's rows alone average ~7KB each, far above most other
  // texts'), so this bin-packs by total *character count* rather than row
  // count — row count alone under-weighted collections made of few but very
  // long rows, leaving their worker the slowest shard even when every
  // worker had a similar row total. Every worker gets a 'load' call even
  // when its slice is empty, so a previous, larger assignment gets cleared.
  function loadSourceIntoWorkers(rows) {
    if (!searchWorkers.length) return;
    const bySourceId = {};
    rows.forEach(r => { (bySourceId[r.sourceId] = bySourceId[r.sourceId] || []).push(r); });

    const weighOf = (sourceRows) => sourceRows.reduce((sum, r) => sum + (r.text ? r.text.length : 0), 0);
    const buckets = searchWorkers.map(() => ({ rows: [], weight: 0 }));
    Object.values(bySourceId)
      .sort((a, b) => weighOf(b) - weighOf(a)) // largest collections placed first for better packing
      .forEach((sourceRows) => {
        const lightest = buckets.reduce((min, b) => (b.weight < min.weight ? b : min), buckets[0]);
        lightest.rows.push(...sourceRows);
        lightest.weight += weighOf(sourceRows);
      });

    buckets.forEach((bucket, i) => {
      searchWorkers[i].postMessage({ type: 'load', name: 'source', data: bucket.rows });
    });
  }

  function buildBibleIndex(verses) {
    allBibleVerses = verses;
    bibleIndexReady = true;
    buildRealWordSet(verses);
    setBibleStatus('ready', `Full Bible loaded — ${verses.length.toLocaleString()} verses searchable offline`);
    loadBibleIntoWorkers(verses);
    if (searchInput.value.trim()) render();
    renderReadSourcePicker();
  }

  async function downloadFullBible() {
    setBibleStatus('downloading', 'Downloading full Bible for offline use…');
    const flatVerses = [];
    let failedBooks = 0;
    await Promise.all(BIBLE_BOOKS.map(async (b) => {
      try {
        const res = await fetch(`https://cdn.jsdelivr.net/gh/aruljohn/Bible-kjv@a9aa4e55afbb3e095f57e4b14cd1f22c5ee8d7c9/${b}.json`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const book = await res.json();
        (book.chapters || []).forEach(ch => {
          (ch.verses || []).forEach(v => {
            flatVerses.push({ ref: `${book.book} ${ch.chapter}:${v.verse}`, text: v.text });
          });
        });
      } catch (err) {
        failedBooks++;
      }
    }));

    if (flatVerses.length === 0) {
      setBibleStatus('offline-empty', 'Could not download Scripture data — check connection and reload.');
      return;
    }

    try {
      await saveVersesToDB(flatVerses);
    } catch (err) {
      console.warn('TEP: failed to persist Bible verses to IndexedDB:', err);
    }
    buildBibleIndex(flatVerses);
    if (failedBooks > 0) {
      setBibleStatus('ready', `Full Bible loaded — ${flatVerses.length.toLocaleString()} verses searchable (${failedBooks} book${failedBooks === 1 ? '' : 's'} failed to download — try Recheck Sources later)`);
    }
  }

  async function initBibleData() {
    const cachedCount = await getCachedVerseCount();
    if (cachedCount > 0) {
      const verses = await loadAllVersesFromDB();
      buildBibleIndex(verses);
    } else if (navigator.onLine) {
      await downloadFullBible();
    } else {
      setBibleStatus('offline-empty', 'No signal yet — connect once to download the full Bible for offline use.');
    }
  }

  /* ================= Source-text storage / index ================= */

  const SRC_DB_NAME = 'tep-source-texts';
  const SRC_STORE = 'texts';
  const SRC_META_STORE = 'meta';
  const sourceStatus = {}; // id -> 'ready' | 'failed' | 'loading' | 'missing'
  const sourceByteSize = {}; // id -> approx bytes currently stored in IndexedDB

  // Traditions whose texts download automatically on first load. Anything
  // else (future additions — new traditions, alternate-language editions)
  // is opt-in only, downloaded from Settings -> Source Texts.
  const AUTO_DOWNLOAD_TRADITIONS = new Set(['Islam', 'Hinduism', 'Buddhism', 'Catholicism', 'Apocrypha']);

  // User overrides of the default install state, keyed by tradition name:
  // true = keep installed, false = removed from device. Absence means "use
  // the AUTO_DOWNLOAD_TRADITIONS default." Persisted so an uninstall (or an
  // opt-in install) survives a reload instead of silently reverting.
  let traditionOverrides = {};

  function isTraditionWanted(tradition) {
    if (Object.prototype.hasOwnProperty.call(traditionOverrides, tradition)) {
      return traditionOverrides[tradition];
    }
    return AUTO_DOWNLOAD_TRADITIONS.has(tradition);
  }

  async function setTraditionOverride(tradition, wanted) {
    traditionOverrides[tradition] = wanted;
    await setSetting('tradition-overrides', traditionOverrides);
  }

  function formatBytes(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  }

  // Bump this whenever a source's fetch/parse logic changes in a way that
  // could make previously-cached rows wrong or stale (e.g. the Dhammapada
  // fix that stopped picking up raw HTML/placeholder markup). A version
  // bump makes initSourceTexts() wipe IndexedDB and re-fetch everything
  // once, instead of silently keeping old bad data forever.
  const SOURCE_SCHEMA_VERSION = 2;

  function openSourceDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(SRC_DB_NAME, 2);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(SRC_STORE)) {
          const s = db.createObjectStore(SRC_STORE, { keyPath: 'key' });
          s.createIndex('sourceId', 'sourceId');
        }
        if (!db.objectStoreNames.contains(SRC_META_STORE)) {
          db.createObjectStore(SRC_META_STORE, { keyPath: 'key' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function getSourceSchemaVersion() {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_META_STORE, 'readonly');
      const req = tx.objectStore(SRC_META_STORE).get('schemaVersion');
      req.onsuccess = () => resolve(req.result ? req.result.value : null);
      req.onerror = () => resolve(null);
    });
  }

  async function setSourceSchemaVersion(version) {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_META_STORE, 'readwrite');
      tx.objectStore(SRC_META_STORE).put({ key: 'schemaVersion', value: version });
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  }

  async function saveSourceRows(sourceId, rows) {
    const db = await openSourceDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(SRC_STORE, 'readwrite');
      const store = tx.objectStore(SRC_STORE);
      rows.forEach(r => store.put({
        key: `${sourceId}|${r.ref}`,
        sourceId,
        ref: r.ref,
        text: r.text
      }));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async function loadAllSourceRows() {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_STORE, 'readonly');
      const req = tx.objectStore(SRC_STORE).getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => resolve([]);
    });
  }

  async function clearAllSourceRows() {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_STORE, 'readwrite');
      tx.objectStore(SRC_STORE).clear();
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  }

  async function deleteSourceRowsForIds(sourceIds) {
    const db = await openSourceDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(SRC_STORE, 'readwrite');
      const store = tx.objectStore(SRC_STORE);
      const index = store.index('sourceId');
      sourceIds.forEach(id => {
        const cursorReq = index.openKeyCursor(IDBKeyRange.only(id));
        cursorReq.onsuccess = (e) => {
          const cursor = e.target.result;
          if (cursor) {
            store.delete(cursor.primaryKey);
            cursor.continue();
          }
        };
      });
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  function buildSourceIndex(rows) {
    allSourceRows = rows;
    sourceIndexReady = rows.length > 0;
    Object.keys(sourceByteSize).forEach(k => delete sourceByteSize[k]);
    rows.forEach(r => {
      sourceByteSize[r.sourceId] = (sourceByteSize[r.sourceId] || 0) + (r.text ? r.text.length : 0) + (r.ref ? r.ref.length : 0);
    });
    loadSourceIntoWorkers(rows); // always runs, even for [] — clears every worker's stale indices after an uninstall
    if (!rows.length) { renderReadSourcePicker(); return; }
    if (searchInput.value.trim()) render();
    renderReadSourcePicker();
  }

  // Records exactly what happened on every fetch attempt, so a failure can be
  // diagnosed instead of guessed at. Keyed by source id.
  const sourceDiagnostics = {};

  function classifyError(err) {
    const msg = String((err && err.message) || err || 'unknown');
    // A network-level fetch rejection (as opposed to an HTTP error status) in a
    // browser almost always means CORS or DNS/offline, not a wrong path.
    if (/Failed to fetch|NetworkError|Load failed/i.test(msg)) {
      return 'BLOCKED — CORS or network (not a bad path)';
    }
    if (/Unexpected token|JSON|SyntaxError/i.test(msg)) {
      return 'NOT JSON — got HTML/other (usually a wrong path)';
    }
    return msg;
  }

  async function fetchSource(source) {
    const attempts = [];
    sourceDiagnostics[source.id] = attempts;

    // Some sources need multiple requests (e.g. one per chapter) and supply
    // their own fetch routine, returning rows directly.
    if (source.fetchRows) {
      try {
        const rows = await source.fetchRows();
        attempts.push({ url: '(multi-request)', result: `${rows ? rows.length : 0} rows` });
        if (!rows || rows.length === 0) throw new Error('no rows returned');
        return rows;
      } catch (err) {
        attempts.push({ url: '(multi-request)', result: classifyError(err) });
        throw err;
      }
    }

    // Resolve candidate URLs (some sources discover their URL dynamically).
    let urls = source.urls || [];
    if (source.discover) {
      try {
        urls = await source.discover();
        attempts.push({ url: '(discovery)', result: `resolved ${urls.length} candidate URL(s)` });
      } catch (err) {
        attempts.push({ url: '(discovery)', result: classifyError(err) });
        throw new Error(`could not resolve source URL: ${classifyError(err)}`);
      }
    }

    if (!urls.length) {
      attempts.push({ url: '(none)', result: 'no candidate URLs' });
      throw new Error('no candidate URLs');
    }

    let lastErr = null;
    for (const url of urls) {
      try {
        const res = await fetch(url);
        if (!res.ok) {
          attempts.push({ url, result: `HTTP ${res.status}` });
          lastErr = new Error(`HTTP ${res.status}`);
          continue;
        }
        const data = await res.json();
        const rows = source.parse(data);
        if (rows.length > 0) {
          attempts.push({ url, result: `OK — ${rows.length} verses` });
          return rows;
        }
        // Reached the file but got nothing usable: show the payload's top-level
        // keys, which is exactly what's needed to fix the parser.
        const shape = data && typeof data === 'object'
          ? Object.keys(data).slice(0, 6).join(', ') || '(empty object)'
          : typeof data;
        attempts.push({ url, result: `parsed 0 rows — payload keys: [${shape}]` });
        lastErr = new Error('parsed 0 rows');
      } catch (err) {
        attempts.push({ url, result: classifyError(err) });
        lastErr = err;
      }
    }
    throw lastErr || new Error('all URLs failed');
  }

  async function downloadSourceTexts(targets) {
    const list = targets || SOURCE_TEXTS;
    const results = await Promise.all(list.map(async (source) => {
      sourceStatus[source.id] = 'loading';
      try {
        const rows = await fetchSource(source);
        await saveSourceRows(source.id, rows);
        sourceStatus[source.id] = 'ready';
        return { id: source.id, count: rows.length };
      } catch (err) {
        // Visible failure, never silent — app keeps working without this source.
        console.warn(`TEP: source "${source.id}" unavailable:`, err.message);
        sourceStatus[source.id] = 'failed';
        return { id: source.id, count: 0, error: err.message };
      }
    }));

    const all = await loadAllSourceRows();
    buildSourceIndex(all);
    renderSourceStatusPanel();
    return results;
  }

  async function initSourceTexts() {
    const storedSchemaVersion = await getSourceSchemaVersion();
    if (storedSchemaVersion !== SOURCE_SCHEMA_VERSION) {
      await clearAllSourceRows();
      await setSourceSchemaVersion(SOURCE_SCHEMA_VERSION);
    }

    traditionOverrides = (await getSetting('tradition-overrides')) || {};

    const cached = await loadAllSourceRows();
    if (cached.length > 0) {
      const present = new Set(cached.map(r => r.sourceId));
      SOURCE_TEXTS.forEach(s => { sourceStatus[s.id] = present.has(s.id) ? 'ready' : 'missing'; });
      buildSourceIndex(cached);
    } else {
      SOURCE_TEXTS.forEach(s => { sourceStatus[s.id] = 'missing'; });
    }
    renderSourceStatusPanel();

    if (navigator.onLine) {
      const toFetch = SOURCE_TEXTS.filter(s => sourceStatus[s.id] !== 'ready' && isTraditionWanted(s.tradition));
      if (toFetch.length) downloadSourceTexts(toFetch);
    }
  }

  async function installTradition(tradition) {
    if (!navigator.onLine) { showToast('You appear to be offline'); return; }
    await setTraditionOverride(tradition, true);
    const targets = SOURCE_TEXTS.filter(s => s.tradition === tradition);
    targets.forEach(s => { sourceStatus[s.id] = 'loading'; });
    renderSourceStatusPanel();
    await downloadSourceTexts(targets);
    const okCount = targets.filter(s => sourceStatus[s.id] === 'ready').length;
    showToast(okCount === targets.length
      ? `${tradition} downloaded — searchable offline`
      : `${tradition}: ${okCount} of ${targets.length} texts downloaded`);
  }

  async function uninstallTradition(tradition) {
    const ids = SOURCE_TEXTS.filter(s => s.tradition === tradition).map(s => s.id);
    // Flip status and re-render immediately, before the async delete
    // resolves — otherwise the "Remove from device" button stays live
    // during the await and a second click re-enters this function for the
    // same tradition (harmless, since the delete is idempotent, but wasteful).
    ids.forEach(id => { sourceStatus[id] = 'removing'; });
    renderSourceStatusPanel();
    await deleteSourceRowsForIds(ids);
    ids.forEach(id => { sourceStatus[id] = 'missing'; delete sourceDiagnostics[id]; });
    await setTraditionOverride(tradition, false);
    const all = await loadAllSourceRows();
    buildSourceIndex(all);
    renderSourceStatusPanel();
    showToast(`${tradition} removed from this device — download it again anytime`);
  }

  // Display order for tradition groups — SOURCE_TEXTS insertion order, deduped.
  function sourceTraditionOrder() {
    const seen = [];
    SOURCE_TEXTS.forEach(s => { if (!seen.includes(s.tradition)) seen.push(s.tradition); });
    return seen;
  }

  function renderSourceStatusPanel() {
    const el = document.getElementById('source-status-list');
    if (!el) return;

    el.innerHTML = sourceTraditionOrder().map(tradition => {
      const sources = SOURCE_TEXTS.filter(s => s.tradition === tradition);
      const wanted = isTraditionWanted(tradition);
      const statuses = sources.map(s => sourceStatus[s.id] || 'missing');
      const readyCount = statuses.filter(st => st === 'ready').length;
      const anyLoading = statuses.includes('loading');
      const anyRemoving = statuses.includes('removing');
      const allReady = readyCount === sources.length;

      let dotClass, summary;
      if (anyRemoving) {
        dotClass = 'downloading'; summary = 'Removing…';
      } else if (anyLoading) {
        dotClass = 'downloading'; summary = 'Downloading…';
      } else if (allReady) {
        dotClass = 'ready'; summary = `Downloaded — ${readyCount} text${readyCount === 1 ? '' : 's'} searchable offline`;
      } else if (readyCount > 0) {
        dotClass = 'offline-empty'; summary = `Partially downloaded — ${readyCount} of ${sources.length} texts`;
      } else {
        dotClass = 'offline-empty'; summary = wanted ? 'Not downloaded yet' : 'Not downloaded — optional';
      }

      const bytes = sources.reduce((sum, s) => sum + (sourceByteSize[s.id] || 0), 0);
      const sizeLabel = bytes > 0 ? ` (${formatBytes(bytes)} on device)` : '';

      const actionBtn = (anyLoading || anyRemoving) ? ''
        : allReady
          ? `<button class="contact-btn source-action-btn" data-action="uninstall" data-tradition="${escapeHtml(tradition)}">Remove from device</button>`
          : `<button class="submit-btn source-action-btn" data-action="install" data-tradition="${escapeHtml(tradition)}">Download</button>`;

      const licenses = [...new Set(sources.map(s => s.license))].join(' · ');

      const filesDetail = sources.map(s => {
        const st = sourceStatus[s.id] || 'missing';
        const label = st === 'ready' ? 'Loaded' : st === 'loading' ? 'Downloading…' : st === 'removing' ? 'Removing…' : st === 'failed' ? 'Unavailable' : 'Not downloaded';
        const attempts = sourceDiagnostics[s.id] || [];
        const diag = attempts.length
          ? `<div class="source-diag">${attempts.map(a => `
               <div class="diag-line">
                 <span class="diag-url">${escapeHtml(a.url.length > 72 ? a.url.slice(0, 69) + '…' : a.url)}</span>
                 <span class="diag-result">${escapeHtml(a.result)}</span>
               </div>`).join('')}</div>`
          : '';
        return `<div class="source-file-row"><span>${escapeHtml(s.label)}</span><span>${label}</span></div>${diag}`;
      }).join('');

      return `
        <div class="source-status-row">
          <div class="source-status-head">
            <p class="bible-status ${dotClass}" style="margin:0;"><span class="dot"></span><strong>${escapeHtml(tradition)}</strong> — ${summary}${sizeLabel}</p>
            ${actionBtn}
          </div>
          <p class="source-license">${escapeHtml(licenses)}</p>
          <details class="source-file-details"><summary>${sources.length} text${sources.length === 1 ? '' : 's'}</summary>${filesDetail}</details>
        </div>`;
    }).join('');

    el.querySelectorAll('[data-action="uninstall"]').forEach(btn => {
      btn.addEventListener('click', () => uninstallTradition(btn.dataset.tradition));
    });
    el.querySelectorAll('[data-action="install"]').forEach(btn => {
      btn.addEventListener('click', () => installTradition(btn.dataset.tradition));
    });
  }

  /* Builds a plain-text report of every fetch attempt — easy to copy and send. */
  function buildDiagnosticReport() {
    const lines = ['TEP source diagnostics', new Date().toISOString(), ''];
    SOURCE_TEXTS.forEach(s => {
      lines.push(`[${(sourceStatus[s.id] || 'missing').toUpperCase()}] ${s.label} (${s.tradition})`);
      const attempts = sourceDiagnostics[s.id] || [];
      if (!attempts.length) lines.push('  (no attempts recorded)');
      attempts.forEach(a => lines.push(`  ${a.url}\n    -> ${a.result}`));
      lines.push('');
    });
    return lines.join('\n');
  }

  async function recheckSources() {
    if (!navigator.onLine) {
      showToast('You appear to be offline');
      return;
    }
    const targets = SOURCE_TEXTS.filter(s => isTraditionWanted(s.tradition));
    targets.forEach(s => { sourceStatus[s.id] = 'loading'; });
    renderSourceStatusPanel();
    await downloadSourceTexts(targets);
    const okCount = targets.filter(s => sourceStatus[s.id] === 'ready').length;
    showToast(`${okCount} of ${targets.length} installed sources loaded`);
  }

  /* Renders matches from other traditions' texts, showing only the text
     tied to whichever religion is selected in the sidebar (or all of them
     when "All" is selected). The sidebar is the single show/hide control. */
  function renderSourceResults(query, sourceResult) {
    const section = document.getElementById('source-results-section');
    const container = document.getElementById('source-results');
    const heading = document.getElementById('source-results-heading');
    if (!section || !container) return;

    if (SOURCE_TEXTS.length === 0) { section.style.display = 'none'; return; }

    // A tradition (e.g. Islam) can now have several source texts (Qur'an
    // plus the six hadith collections), so filtering is by a *list* of
    // matching sources, not a single one — narrowed further to just one
    // specific text when the text-filter dropdown picks one out.
    const matchingSources = activeSourceId
      ? SOURCE_TEXTS.filter(s => s.id === activeSourceId)
      : (activeFilter === 'all' ? SOURCE_TEXTS : SOURCE_TEXTS.filter(s => s.tradition === activeFilter));
    const filteringToUncoveredReligion = activeFilter !== 'all' && matchingSources.length === 0;

    if (heading) {
      heading.textContent = filteringToUncoveredReligion
        ? `From ${activeFilter}'s own text`
        : activeSourceId
          ? `From ${matchingSources[0] ? matchingSources[0].label : activeFilter}`
          : (activeFilter === 'all'
              ? `From other traditions' own texts`
              : `From ${activeFilter}'s own text${matchingSources.length > 1 ? 's' : ''}`);
    }

    // The sidebar has a religion selected that has no primary text loaded
    // (e.g. Atheism, Humanism) — hide the results list and say so plainly.
    if (filteringToUncoveredReligion) {
      section.style.display = 'block';
      container.innerHTML = `<p class="source-tab-note">No primary source text is loaded for ${escapeHtml(activeFilter)} yet. Select "All" or a tradition with a loaded text.</p>`;
      return;
    }

    // A religion is selected but none of its sources have finished loading —
    // say that too, even before anything has been typed.
    const readySources = matchingSources.filter(s => sourceStatus[s.id] === 'ready');
    if (activeFilter !== 'all' && readySources.length === 0) {
      section.style.display = 'block';
      const anyLoading = matchingSources.some(s => sourceStatus[s.id] === 'loading');
      const reason = anyLoading ? 'is still downloading' : "hasn't loaded yet";
      const label = matchingSources.length === 1 ? matchingSources[0].label : `${activeFilter}'s texts`;
      container.innerHTML = `<p class="source-tab-note">${escapeHtml(label)} ${reason}. Check Settings → Source Texts to re-check, or try again once you're online.</p>`;
      return;
    }

    if (!query.trim() || !sourceIndexReady) {
      section.style.display = 'none';
      container.innerHTML = '';
      return;
    }

    // Tradition filtering (by readySources' ids) already happened inside the
    // worker via sourceIdWhitelist, so sourceResult is ready to use as-is.
    if (sourceResult.total === 0) {
      section.style.display = 'block';
      container.innerHTML = `<p class="source-tab-note">No matches for &ldquo;${escapeHtml(query)}&rdquo;${activeFilter !== 'all' ? ' in this text' : ''}.</p>`;
      return;
    }
    const matches = sourceResult.top.slice(0, 25).map(m => m.item);
    section.style.display = 'block';

    // Group by tradition so it reads as "what each religion's own text says".
    const byTradition = {};
    matches.forEach(m => {
      const src = SOURCE_TEXTS.find(s => s.id === m.sourceId);
      const trad = src ? src.tradition : 'Other';
      (byTradition[trad] = byTradition[trad] || []).push(m);
    });

    const countHtml = `<p class="results-count">Results 1-${matches.length} of <strong>${sourceResult.total.toLocaleString()}</strong> for &ldquo;${escapeHtml(query)}&rdquo;</p>`;

    container.innerHTML = countHtml + Object.entries(byTradition).map(([tradition, rows]) => `
      <div class="source-group">
        <div class="source-group-label">${escapeHtml(tradition)}</div>
        ${rows.map(r => `
          <div class="bible-result-item source-item">
            <span class="ref">${escapeHtml(r.ref)}</span>
            <p>"${highlightText(escapeHtml(r.text), query)}"</p>
          </div>
        `).join('')}
      </div>
    `).join('');
  }

  /* ================= Read Full Texts ================= */
  /*
   * Lets a user page through an entire loaded text (KJV Bible, Qur'an,
   * Bhagavad Gita, Dhammapada) chapter by chapter, instead of only ever
   * seeing a search hit or the single verse cited in a case card.
   */

  const READ_SOURCES = [
    { key: 'bible', label: 'Bible (KJV)', tradition: 'Christianity' },
    { key: 'quran', label: "Qur'an", tradition: 'Islam' },
    { key: 'hadith-bukhari', label: 'Sahih al-Bukhari', tradition: 'Islam' },
    { key: 'hadith-muslim', label: 'Sahih Muslim', tradition: 'Islam' },
    { key: 'hadith-abudawud', label: 'Sunan Abu Dawud', tradition: 'Islam' },
    { key: 'hadith-tirmidhi', label: 'Jami at-Tirmidhi', tradition: 'Islam' },
    { key: 'hadith-nasai', label: "Sunan an-Nasa'i", tradition: 'Islam' },
    { key: 'hadith-ibnmajah', label: 'Sunan Ibn Majah', tradition: 'Islam' },
    { key: 'gita', label: 'Bhagavad Gita', tradition: 'Hinduism' },
    { key: 'yoga-sutras', label: 'Yoga Sutras of Patanjali', tradition: 'Hinduism' },
    { key: 'brahma-sutras', label: 'Brahma Sutras', tradition: 'Hinduism' },
    { key: 'upanishads', label: 'The Principal Upanishads', tradition: 'Hinduism' },
    { key: 'ramayana', label: 'The Ramayana', tradition: 'Hinduism' },
    { key: 'vishnu-purana', label: 'Vishnu Purana', tradition: 'Hinduism' },
    { key: 'manusmriti', label: 'Manusmriti (Laws of Manu)', tradition: 'Hinduism' },
    { key: 'mahabharata', label: 'The Mahabharata', tradition: 'Hinduism' },
    { key: 'dhammapada', label: 'Dhammapada', tradition: 'Buddhism' },
    { key: 'digha-nikaya', label: 'Digha Nikaya (Long Discourses)', tradition: 'Buddhism' },
    { key: 'majjhima-nikaya', label: 'Majjhima Nikaya (Middle Discourses)', tradition: 'Buddhism' },
    { key: 'sutta-nipata', label: 'Sutta Nipata', tradition: 'Buddhism' },
    { key: 'udana', label: 'Udana', tradition: 'Buddhism' },
    { key: 'itivuttaka', label: 'Itivuttaka', tradition: 'Buddhism' },
    { key: 'sn-an-highlights', label: 'Selected Suttas (Samyutta & Anguttara)', tradition: 'Buddhism' },
    { key: 'heart-sutra', label: 'Heart Sutra', tradition: 'Buddhism' },
    { key: 'baltimore-catechism', label: 'Baltimore Catechism', tradition: 'Catholicism' },
    { key: 'deuterocanon', label: 'Apocrypha / Deuterocanon', tradition: 'Apocrypha' },
    { key: 'trent-canons', label: 'Council of Trent — Canons', tradition: 'Catholicism' },
    { key: 'rerum-novarum', label: 'Rerum Novarum (1891)', tradition: 'Catholicism' },
    { key: 'ineffabilis-deus', label: 'Ineffabilis Deus (1854)', tradition: 'Catholicism' },
    { key: 'summa-theologica', label: 'Summa Theologica', tradition: 'Catholicism' },
    { key: 'yasna', label: 'The Yasna', tradition: 'Zoroastrianism' },
    { key: 'key-to-theosophy', label: 'The Key to Theosophy', tradition: 'Theosophy' },
    { key: 'secret-doctrine', label: 'The Secret Doctrine', tradition: 'Theosophy' },
    { key: 'book-of-the-law', label: 'The Book of the Law', tradition: 'Thelema' },
    { key: 'pistis-sophia', label: 'Pistis Sophia (Horner, 1924)', tradition: 'Gnosticism' },
    { key: 'corpus-hermeticum', label: 'Corpus Hermeticum (Mead, 1906)', tradition: 'Gnosticism' },
    { key: 'setna-magic-book', label: 'Setna and the Magic Book (Petrie, 1895)', tradition: 'Gnosticism' },
    { key: 'pyramid-texts', label: 'The Pyramid Texts (Mercer, 1952)', tradition: 'Egyptian Occultism' },
    { key: 'book-of-the-dead', label: 'The Book of the Dead (Budge, 1895)', tradition: 'Egyptian Occultism' },
    { key: 'demotic-magical-papyrus', label: 'Demotic Magical Papyrus of London and Leiden', tradition: 'Egyptian Occultism' },
    { key: 'goodwin-pgm', label: 'Graeco-Egyptian Magic Papyrus (Goodwin, 1852)', tradition: 'Egyptian Occultism' },
    { key: 'egyptian-magic', label: 'Egyptian Magic (Budge, 1899)', tradition: 'Egyptian Occultism' },
    { key: 'kybalion', label: 'The Kybalion', tradition: 'Gnosticism' },
    { key: 'emerald-tablet', label: 'The Emerald Tablet', tradition: 'Gnosticism' },
    { key: 'yoruba-religion', label: 'Yoruba Religion (Ellis, 1894)', tradition: 'Yoruba Religion' },
    { key: 'vodou', label: 'The Magic Island (Seabrook, 1929)', tradition: 'Vodou' },
    { key: 'tetrabiblos', label: "Ptolemy's Tetrabiblos (Ashmand, 1822)", tradition: 'Astrology' }
  ];

  // Display order for the religion picker — READ_SOURCES insertion order
  // already follows this, but keep it explicit in case entries get reordered.
  const READ_RELIGION_ORDER = ['Christianity', 'Catholicism', 'Apocrypha', 'Islam', 'Hinduism', 'Buddhism', 'Zoroastrianism', 'Theosophy', 'Thelema', 'Gnosticism', 'Egyptian Occultism', 'Yoruba Religion', 'Vodou', 'Astrology'];

  // Canonical KJV reading order — BIBLE_BOOKS (declared earlier, for CDN
  // fetch filenames) is alphabetical, which would list books completely out
  // of reading order in a book picker.
  const BIBLE_BOOK_ORDER = [
    "Genesis","Exodus","Leviticus","Numbers","Deuteronomy","Joshua","Judges","Ruth",
    "1Samuel","2Samuel","1Kings","2Kings","1Chronicles","2Chronicles","Ezra","Nehemiah",
    "Esther","Job","Psalms","Proverbs","Ecclesiastes","SongofSolomon","Isaiah","Jeremiah",
    "Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah","Jonah","Micah",
    "Nahum","Habakkuk","Zephaniah","Haggai","Zechariah","Malachi",
    "Matthew","Mark","Luke","John","Acts","Romans","1Corinthians","2Corinthians",
    "Galatians","Ephesians","Philippians","Colossians","1Thessalonians","2Thessalonians",
    "1Timothy","2Timothy","Titus","Philemon","Hebrews","James","1Peter","2Peter",
    "1John","2John","3John","Jude","Revelation"
  ];

  let readReligionKey = null; // Selected religion — the dropdown lists this religion's texts
  let readSourceKey = null;
  let readBookKey = null;   // Bible only: selected book name, as it appears in the data
  let readGroupKey = null;  // Bible: selected chapter number; others: surah/chapter/verse-range label

  function readSourceReady(key) {
    if (key === 'bible') return allBibleVerses.length > 0;
    return sourceStatus[key] === 'ready';
  }

  function readSourceRows(key) {
    if (key === 'bible') return allBibleVerses;
    return allSourceRows.filter(r => r.sourceId === key);
  }

  // Pulls a stable, sortable descriptor out of each ref format.
  function parseReadRef(sourceKey, ref) {
    if (sourceKey === 'bible') {
      const m = ref.match(/^(.*)\s(\d+):(\d+)$/);
      if (!m) return null;
      return { book: m[1], chapter: parseInt(m[2], 10), verse: parseInt(m[3], 10) };
    }
    if (sourceKey === 'quran') {
      const m = ref.match(/^Qur'an (\d+):(\d+)$/i);
      if (!m) return null;
      return { group: `Surah ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'gita') {
      const m = ref.match(/^Bhagavad Gita (\d+):(\d+)$/);
      if (m) return { group: `Chapter ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
      return { group: 'Other', chapter: 999, verse: 0 };
    }
    if (sourceKey === 'dhammapada') {
      const m = ref.match(/^Dhammapada (\d+)$/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 50) * 50 + 1;
      return { group: `Verses ${start}–${start + 49}`, chapter: start, verse: n };
    }
    if (sourceKey === 'manusmriti') {
      const m = ref.match(/^Manusmriti (\d+):(\d+)/);
      if (!m) return null;
      return { group: `Chapter ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'yoga-sutras') {
      const m = ref.match(/^Yoga Sutras (\d+)\.(\d+)$/);
      if (!m) return null;
      const PADA_NAMES = ['', 'Samadhi Pada', 'Sadhana Pada', 'Vibhuti Pada', 'Kaivalya Pada'];
      return { group: PADA_NAMES[parseInt(m[1], 10)] || `Pada ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'mahabharata') {
      const m = ref.match(/^Mahabharata, (\w+) Parva, Section (\d+)$/);
      if (!m) return null;
      const PARVA_ORDER = [
        'Adi', 'Sabha', 'Vana', 'Virata', 'Udyoga', 'Bhishma', 'Drona', 'Karna',
        'Shalya', 'Sauptika', 'Stri', 'Shanti', 'Anushasana', 'Ashvamedhika',
        'Ashramavasika', 'Mausala', 'Mahaprasthanika', 'Svargarohana'
      ];
      const idx = PARVA_ORDER.indexOf(m[1]);
      return { group: `${m[1]} Parva`, chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'upanishads') {
      const m = ref.match(/^(.+) Upanishad (\d+)$/);
      if (!m) return null;
      const UPANISHAD_ORDER = [
        'Brihadaranyaka', 'Chandogya', 'Taittiriya', 'Aitareya', 'Kaushitaki',
        'Kena', 'Katha', 'Isha', 'Mundaka', 'Prashna', 'Mandukya',
        'Svetasvatara', 'Maitri'
      ];
      const idx = UPANISHAD_ORDER.indexOf(m[1]);
      return { group: `${m[1]} Upanishad`, chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'ramayana') {
      const m = ref.match(/^Ramayana, (\w+) Kanda, Canto (\d+)$/);
      if (!m) return null;
      const KANDA_ORDER = ['Bala', 'Ayodhya', 'Aranya', 'Kishkindha', 'Sundara', 'Yuddha'];
      const idx = KANDA_ORDER.indexOf(m[1]);
      return { group: `${m[1]} Kanda`, chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'vishnu-purana') {
      const m = ref.match(/^Vishnu Purana (\d+)\.(\d+)$/);
      if (!m) return null;
      return { group: `Book ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'brahma-sutras') {
      const m = ref.match(/^Brahma Sutras (\d+)\.(\d+)\.(\d+)$/);
      if (!m) return null;
      const adhyaya = parseInt(m[1], 10), pada = parseInt(m[2], 10);
      return {
        group: `Adhyaya ${m[1]}, Pada ${m[2]}`,
        chapter: adhyaya * 10 + pada,
        verse: parseInt(m[3], 10)
      };
    }
    if (sourceKey === 'digha-nikaya' || sourceKey === 'majjhima-nikaya') {
      const label = sourceKey === 'digha-nikaya' ? 'Digha Nikaya' : 'Majjhima Nikaya';
      const titles = sourceKey === 'digha-nikaya' ? DN_TITLES : MN_TITLES;
      const m = ref.match(new RegExp(`^${label} (\\d+)\\.(\\d+)$`));
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const title = titles[n] || '';
      return { group: `${n}. ${title}`, chapter: n, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'heart-sutra') {
      const m = ref.match(/^Heart Sutra \((\w+) Recension\) (\d+)$/);
      if (!m) return null;
      return { group: `${m[1]} Recension`, chapter: m[1] === 'Smaller' ? 1 : 2, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'sn-an-highlights') {
      const m = ref.match(/^(.+) (\d+)$/);
      if (!m) return null;
      const idx = SN_AN_HIGHLIGHTS.findIndex(t => t.label === m[1]);
      return { group: m[1], chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'sutta-nipata') {
      const m = ref.match(/^Sutta Nipata (\d+)\.(\d+)$/);
      if (!m) return null;
      const SNP_VAGGA_NAMES = ['', 'Uragavagga (Serpent)', 'Culavagga (Minor Chapter)', 'Mahavagga (Great Chapter)', 'Atthakavagga (Chapter of Eights)', 'Parayanavagga (The Way to the Far Shore)'];
      const v = parseInt(m[1], 10);
      return { group: SNP_VAGGA_NAMES[v] || `Vagga ${v}`, chapter: v, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'udana') {
      const m = ref.match(/^Udana (\d+)\.(\d+)$/);
      if (!m) return null;
      const UD_VAGGA_NAMES = ['', 'Bodhivagga', 'Mucalindavagga', 'Nandavagga', 'Meghiyavagga', 'Sonavagga', 'Jaccandhavagga', 'Culavagga', 'Pataligamiyavagga'];
      const v = parseInt(m[1], 10);
      return { group: UD_VAGGA_NAMES[v] || `Vagga ${v}`, chapter: v, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'itivuttaka') {
      const m = ref.match(/^Itivuttaka (\d+)$/);
      if (!m) return null;
      const ITI_VAGGA_BOUNDS = [10, 20, 27, 37, 49, 59, 69, 79, 89, 99, 112];
      const n = parseInt(m[1], 10);
      let vagga = ITI_VAGGA_BOUNDS.length;
      for (let v = 0; v < ITI_VAGGA_BOUNDS.length; v++) {
        if (n <= ITI_VAGGA_BOUNDS[v]) { vagga = v + 1; break; }
      }
      return { group: `Vagga ${vagga}`, chapter: vagga, verse: n };
    }
    if (sourceKey === 'rerum-novarum' || sourceKey === 'ineffabilis-deus') {
      const m = ref.match(/para\.\s*(\d+)$/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 10) * 10 + 1;
      return { group: `¶${start}–${start + 9}`, chapter: start, verse: n };
    }
    if (sourceKey === 'summa-theologica') {
      const m = ref.match(/^Summa Theologica, ([IVX-]+), Q\.\s*(\d+), Art\.\s*(\d+)$/);
      if (!m) return null;
      const SUMMA_PART_ORDER = ['I', 'I-II', 'II-II', 'III'];
      const partIdx = SUMMA_PART_ORDER.indexOf(m[1]);
      return {
        group: `Part ${m[1]}`,
        chapter: partIdx === -1 ? 999 : partIdx,
        verse: parseInt(m[2], 10) * 10000 + parseInt(m[3], 10)
      };
    }
    if (sourceKey === 'trent-canons') {
      const m = ref.match(/^Council of Trent, (.+), Canon ([IVXLCDM]+)$/);
      if (!m) return null;
      const TRENT_GROUP_ORDER = [
        'Session the Sixth (On Justification)',
        'Session the Seventh (On The Sacraments In General)',
        'Session the Seventh (On Baptism)',
        'Session the Seventh (On Confirmation)',
        'Session the Thirteenth (On The Most Holy Sacrament Of The Eucharist)',
        'Session the Fourteenth (On The Most Holy Sacrament Of Penance)',
        'Session the Fourteenth (On The Sacrament Of Extreme Unction)',
        'Session the Twenty-First (On Communion Under Both Species, And On The Communion Of Infants)',
        'Session the Twenty-Second (On The Sacrifice Of The Mass)',
        'Session the Twenty-Third (On The Sacrament Of Order)',
        'Session the Twenty-Fourth (On The Sacrament Of Matrimony)'
      ];
      const ROMAN = { I: 1, V: 5, X: 10, L: 50, C: 100, D: 500, M: 1000 };
      const toInt = (r) => {
        let n = 0;
        for (let i = 0; i < r.length; i++) {
          const cur = ROMAN[r[i]], next = ROMAN[r[i + 1]];
          n += (next && cur < next) ? -cur : cur;
        }
        return n;
      };
      const groupIdx = TRENT_GROUP_ORDER.indexOf(m[1]);
      return { group: m[1], chapter: groupIdx === -1 ? 999 : groupIdx, verse: toInt(m[2]) };
    }
    if (sourceKey === 'deuterocanon') {
      const m = ref.match(/^(.*)\s(\d+):(\d+)$/);
      if (!m) return null;
      // Grouped by whole book (not book+chapter, unlike Bible) since each
      // book is its own "Read Full Text" group here. Two consequences:
      //  - `chapter` becomes canonical book order, so groups sort correctly
      //    instead of tying (every book's own chapter count restarts at 1).
      //  - `verse` becomes a chapter*1000+verse composite, so verses within
      //    a group still sort correctly — renderReadVerses() only sorts by
      //    `verse`, which elsewhere is always scoped to a single chapter.
      const DEUTERO_ORDER = ['Tobias', 'Judith', 'Wisdom', 'Ecclesiasticus', 'Baruch', '1 Machabees', '2 Machabees'];
      const bookIdx = DEUTERO_ORDER.indexOf(m[1]);
      return {
        group: m[1],
        book: m[1],
        chapter: bookIdx === -1 ? 999 : bookIdx,
        verse: parseInt(m[2], 10) * 1000 + parseInt(m[3], 10)
      };
    }
    if (sourceKey === 'kybalion') {
      const m = ref.match(/^The Kybalion, Ch\. (\d+), para\. (\d+)$/);
      if (!m) return null;
      const KYBALION_CHAPTER_TITLES = [
        '', 'The Hermetic Philosophy', 'The Seven Hermetic Principles',
        'Mental Transmutation', 'The All', 'The Mental Universe',
        'The Divine Paradox', '"The All" in All', 'Planes of Correspondence',
        'Vibration', 'Polarity', 'Rhythm', 'Causation', 'Gender',
        'Mental Gender', 'Hermetic Axioms'
      ];
      const n = parseInt(m[1], 10);
      return { group: `Ch. ${n}: ${KYBALION_CHAPTER_TITLES[n] || ''}`, chapter: n, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'emerald-tablet') {
      const m = ref.match(/^Emerald Tablet, v\. (\d+)$/);
      if (!m) return null;
      return { group: 'Emerald Tablet', chapter: 1, verse: parseInt(m[1], 10) };
    }
    if (sourceKey === 'yasna') {
      const m = ref.match(/^Yasna (\d+)\.[\d-]+$/);
      if (!m) return null;
      const YASNA_CHAPTER_TITLES = {
        0: 'Introduction', 3: 'The Objects of Propitiation', 4: 'The Offering Takes Place',
        6: 'The Sacrifice Continues', 7: 'Presentation of Offerings', 8: 'The Meat-Offering',
        11: 'Prelude to the Haoma-Offering', 14: 'Dedications', 15: 'The Sacrifice Continues',
        16: 'The Sacrifice Continues', 17: 'To the Fires, Waters, Plants', 19: 'Commentary on the Ahunwar',
        20: 'Commentary on the Ashem Vohu', 21: 'Commentary on the Yenhe Hatam', 22: 'The Sacrifice Continues',
        23: 'The Fravashis of the Saints', 24: 'Presentations', 26: 'The Fravashis: Sacrifice and Praise',
        27: 'Prelude to the Chief Recital of the Ahunwar', 35: 'Praise to Ahura and the Immortals',
        36: 'To Ahura and the Fire', 37: 'To Ahura, the Holy Creation, and the Fravashis',
        38: 'To the Earth and the Sacred Waters', 39: 'To the Soul of the Kine', 40: 'Prayers for Helpers',
        41: 'A Prayer to Ahura as King, Life, and Rewarder', 42: 'A Supplement to the Haptanghaiti',
        52: 'A Prayer for Sanctity and its Benefits', 54: 'The Airyema-Ishyo',
        55: 'The Worship of the Gathas Concluded', 56: 'Introduction to the Srosh Yasht',
        57: 'The Srosh Yasht', 58: 'The Fshusho Mathra', 59: 'Mutual Blessings',
        60: 'Prayers for the Dwelling of the Sacrificer', 62: 'To the Fire',
        65: 'To Ardvi Sura Anahita, and the Waters', 66: 'To the Ahurian One',
        68: 'To the Ahurian One, and the Waters', 70: 'To the Bountiful Immortals',
        71: 'The Yasna Concluding'
      };
      const n = parseInt(m[1], 10);
      const title = YASNA_CHAPTER_TITLES[n];
      return { group: `Yasna ${n}${title ? ': ' + title : ''}`, chapter: n, verse: parseFloat(ref.match(/\.(\d+)/)[1]) };
    }
    if (sourceKey === 'key-to-theosophy') {
      const m = ref.match(/^The Key to Theosophy, Section (\d+), para\. (\d+)$/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      return { group: `Section ${n}`, chapter: n, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'secret-doctrine') {
      const m = ref.match(/^The Secret Doctrine, (Cosmogenesis|Anthropogenesis), (Proem|Part I|Part II|Part III), para\. (\d+)$/);
      if (!m) return null;
      const VOL_ORDER = { Cosmogenesis: 0, Anthropogenesis: 1 };
      const PART_ORDER = { Proem: 0, 'Part I': 1, 'Part II': 2, 'Part III': 3 };
      return {
        group: `${m[1]} — ${m[2]}`,
        chapter: VOL_ORDER[m[1]] * 10 + PART_ORDER[m[2]],
        verse: parseInt(m[3], 10)
      };
    }
    if (sourceKey === 'book-of-the-law') {
      const m = ref.match(/^The Book of the Law, ([IVX]+):(\d+)$/);
      if (!m) return null;
      const CHAPTER_ORDER = { I: 1, II: 2, III: 3 };
      return { group: `Chapter ${m[1]}`, chapter: CHAPTER_ORDER[m[1]] || 999, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'pistis-sophia') {
      const m = ref.match(/^Pistis Sophia \(Horner, 1924\), (First|Second|Third|Fourth|Fifth) Document, para\. (\d+)$/);
      if (!m) return null;
      const DOC_ORDER = { First: 1, Second: 2, Third: 3, Fourth: 4, Fifth: 5 };
      return { group: `${m[1]} Document`, chapter: DOC_ORDER[m[1]], verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'corpus-hermeticum') {
      const m = ref.match(/^Corpus Hermeticum \(Mead, 1906\), Libellus ([IVX]+), (\d+)$/);
      if (!m) return null;
      const LIBELLUS_ORDER = { I: 1, II: 2, III: 3, IV: 4, V: 5, VI: 6, VII: 7, VIII: 8, IX: 9, X: 10, XI: 11, XII: 12, XIII: 13 };
      return { group: `Libellus ${m[1]}`, chapter: LIBELLUS_ORDER[m[1]] || 999, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'setna-magic-book') {
      const m = ref.match(/^Setna and the Magic Book \(Petrie, 1895\), para\. (\d+)$/);
      if (!m) return null;
      return { group: 'Setna and the Magic Book', chapter: 1, verse: parseInt(m[1], 10) };
    }
    if (sourceKey === 'pyramid-texts') {
      const m = ref.match(/^The Pyramid Texts \(Mercer, 1952\), Utterance (\d+)$/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 50) * 50 + 1;
      return { group: `Utterances ${start}–${start + 49}`, chapter: start, verse: n };
    }
    if (sourceKey === 'book-of-the-dead') {
      const m = ref.match(/^The Book of the Dead \(Budge, 1895\), Plates? ([IVXL]+(?:-[IVXL]+)?)(?:, (?:Ch\. (\S+)|para\. (\d+)))?$/);
      if (!m) return null;
      // Roman-numeral-ish sort key: strip non-letters, use first numeral's
      // rough magnitude via string length + value is overkill here — plate
      // order in the source is already monotonic, so just use first-seen
      // order via a lookup built from PLATE_ORDER below.
      const PLATE_ORDER = {
        'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V-VI': 5, 'VII-X': 6, 'XI-XII': 7,
        'XIII': 8, 'XIV': 9, 'XV': 10, 'XVI': 11, 'XVII': 12, 'XVIII': 13,
        'XIX': 14, 'XX': 15, 'XXI': 16, 'XXII': 17, 'XXIII-XXIV': 18, 'XXV': 19,
        'XXVI': 20, 'XXVII': 21, 'XXVIII': 22, 'XXIX-XXX': 23, 'XXXI-XXXII': 24,
        'XXXII': 25, 'XXXIII': 26, 'XXXIII-XXXIV': 27, 'XXXV-XXXVI': 28, 'XXXVII': 29
      };
      const plate = m[1];
      const label = plate.includes('-') ? `Plates ${plate}` : `Plate ${plate}`;
      return { group: label, chapter: PLATE_ORDER[plate] || 999, verse: parseInt(m[3], 10) || 0 };
    }
    if (sourceKey === 'demotic-magical-papyrus') {
      const m = ref.match(/^The Demotic Magical Papyrus of London and Leiden \(Griffith & Thompson, 1904-1909\), (Verso )?Col\. ([IVXL]+)$/);
      if (!m) return null;
      const ROMAN_ORDER = { I:1,II:2,III:3,IV:4,V:5,VI:6,VII:7,VIII:8,IX:9,X:10,XI:11,XII:12,XIII:13,XIV:14,XV:15,XVI:16,XVII:17,XVIII:18,XIX:19,XX:20,XXI:21,XXII:22,XXIII:23,XXIV:24,XXV:25,XXVI:26,XXVII:27,XXVIII:28,XXIX:29,XXX:30,XXXI:31,XXXII:32,XXXIII:33 };
      const isVerso = !!m[1];
      return { group: isVerso ? 'Verso' : 'Recto', chapter: isVerso ? 1 : 0, verse: ROMAN_ORDER[m[2]] || 999 };
    }
    if (sourceKey === 'goodwin-pgm') {
      const m = ref.match(/^Fragment of a Graeco-Egyptian Work upon Magic \(Goodwin, 1852\), Spell (\d+)$/);
      if (!m) return null;
      return { group: 'Spells', chapter: 1, verse: parseInt(m[1], 10) };
    }
    if (sourceKey === 'egyptian-magic') {
      const m = ref.match(/^Egyptian Magic \(Budge, 1899\), para\. (\d+)$/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 50) * 50 + 1;
      return { group: `¶${start}–${start + 49}`, chapter: start, verse: n };
    }
    if (sourceKey === 'yoruba-religion') {
      const m = ref.match(/^Yoruba Religion \(Ellis, 1894\), Ch\. ([IVX]+), para\. (\d+)$/);
      if (!m) return null;
      const CHAPTER_TITLES = {
        II: 'Chief Gods', III: 'Minor Gods', V: 'Priests and Worship',
        VI: 'Egungun, Oro, Abiku, and Various Superstitions',
        VII: 'The Indwelling Spirits and Souls of Men',
        IX: 'Ceremonies at Birth, Marriage, and Death',
        XIV: 'Folk-Lore Tales'
      };
      const CHAPTER_ORDER = { II: 2, III: 3, V: 5, VI: 6, VII: 7, IX: 9, XIV: 14 };
      const title = CHAPTER_TITLES[m[1]];
      return { group: `Ch. ${m[1]}${title ? ': ' + title : ''}`, chapter: CHAPTER_ORDER[m[1]] || 999, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'vodou') {
      const m = ref.match(/^The Magic Island \(Seabrook, 1929\), (Foreword|Ch\. [IVX]+), para\. (\d+)$/);
      if (!m) return null;
      const CHAPTER_TITLES = {
        'Foreword': 'Foreword', 'Ch. I': 'Secret Fires',
        'Ch. II': 'The Way Is Opened and Closed', 'Ch. III': 'The Petro Sacrifice',
        'Ch. IV': 'The "Ouanga" Charm', 'Ch. V': 'Goat-Cry Girl-Cry',
        'Ch. VI': 'The God Incarnate'
      };
      const CHAPTER_ORDER = {
        'Foreword': 0, 'Ch. I': 1, 'Ch. II': 2, 'Ch. III': 3,
        'Ch. IV': 4, 'Ch. V': 5, 'Ch. VI': 6
      };
      const title = CHAPTER_TITLES[m[1]];
      const group = m[1] === 'Foreword' ? 'Foreword' : `${m[1]}${title ? ': ' + title : ''}`;
      return { group, chapter: CHAPTER_ORDER[m[1]] ?? 999, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'tetrabiblos') {
      const m = ref.match(/^Ptolemy's Tetrabiblos \(Ashmand, 1822\), Book ([IVX]+), Ch\. (\d+), para\. (\d+)$/);
      if (!m) return null;
      const TETRABIBLOS_CHAPTER_TITLES = {
        I: ['', 'Proem', 'Knowledge May Be Acquired by Astronomy to a Certain Extent', 'That Prescience is Useful', 'The Influences of the Planetary Orbs', 'Benefics and Malefics', 'Masculine and Feminine', 'Diurnal and Nocturnal', 'The Influence of Position with Regard to the Sun', 'The Influence of the Fixed Stars', 'Constellations North of the Zodiac', 'Constellations South of the Zodiac', 'The Annual Seasons', 'The Influence of the Four Angles', 'Tropical, Equinoctial, Fixed, and Bicorporeal Signs', 'Masculine and Feminine Signs', 'Mutual Configurations of the Signs', 'Signs Commanding and Obeying', 'Signs Beholding Each Other, and of Equal Power', 'Signs Inconjunct', 'Houses of the Planets', 'The Triplicities', 'Exaltations', 'The Disposition of the Terms', 'The Terms According to Ptolemy', 'The Places and Degrees of Every Planet', 'Faces, Chariots, and Other Similar Attributes of the Planets', 'Application, Separation, and Other Faculties'],
        II: ['', 'General Division of the Subject', 'Peculiarities Observable Throughout Every Entire Climate', 'The Familiarity of the Regions of the Earth with the Triplicities and the Planets', 'The Familiarity of the Regions of the Earth with the Fixed Stars', 'Mode of Particular Prediction in Eclipses', 'The Regions or Countries to Be Considered as Liable to Be Comprehended in the Event', 'The Time and Period of the Event', 'The Genus, Class, or Kind, Liable to Be Affected', 'The Quality and Nature of the Effect', 'Colours in Eclipses; Comets, and Similar Phenomena', 'The New Moon of the Year', 'The Particular Natures of the Signs by Which the Different Constitutions of the Atmosphere Are Produced', 'Mode of Consideration for Particular Constitutions of the Atmosphere', 'The Signification of Meteors'],
        III: ['', 'Proem', 'The Conception and the Parturition, or Birth', 'The Degree Ascending', 'Distribution of the Doctrine of Nativities', 'The Parents', 'Brothers and Sisters', 'Male or Female', 'Twins', 'Monstrous or Defective Births', 'Children Not Reared', 'The Duration of Life', 'The Prorogatory Places', 'The Number of Prorogators, and Also the Part of Fortune', 'Number of the Modes of Prorogation', 'Exemplification', 'The Form and Temperament of the Body', 'The Hurts, Injuries, and Diseases of the Body', 'The Quality of the Mind', 'The Diseases of the Mind'],
        IV: ['', 'Proem', 'The Fortune of Wealth', 'The Fortune of Rank', 'The Quality of Employment', 'Marriage', 'Children', 'Friends and Enemies', 'Travelling', 'The Kind of Death', 'The Periodical Divisions of Time']
      };
      const BOOK_ORDER = { I: 1, II: 2, III: 3, IV: 4 };
      const book = m[1];
      const n = parseInt(m[2], 10);
      const title = (TETRABIBLOS_CHAPTER_TITLES[book] || [])[n];
      return { group: `Book ${book}, Ch. ${n}${title ? ': ' + title : ''}`, chapter: (BOOK_ORDER[book] || 9) * 100 + n, verse: parseInt(m[3], 10) };
    }
    if (sourceKey === 'baltimore-catechism') {
      const m = ref.match(/Q\.\s*(\d+)/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 50) * 50 + 1;
      return { group: `Q. ${start}–${start + 49}`, chapter: start, verse: n };
    }
    if (sourceKey.startsWith('hadith-')) {
      // Ref format is "<Collection name> <book>:<hadith>", e.g. "Sahih al-Bukhari 12:34".
      const m = ref.match(/(\d+):(\d+)$/);
      if (!m) return { group: 'Other', chapter: 999, verse: 0 };
      return { group: `Book ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    return null;
  }

  function readBookOrder() {
    const byNorm = {};
    allBibleVerses.forEach(v => {
      const parsed = parseReadRef('bible', v.ref);
      if (!parsed) return;
      const norm = parsed.book.replace(/\s+/g, '').toLowerCase();
      if (!byNorm[norm]) byNorm[norm] = parsed.book;
    });
    const ordered = BIBLE_BOOK_ORDER.map(b => byNorm[b.toLowerCase()]).filter(Boolean);
    // Anything present in the data but not recognized (unexpected book name) still shows up, at the end.
    const known = new Set(ordered);
    Object.values(byNorm).forEach(b => { if (!known.has(b)) ordered.push(b); });
    return ordered;
  }

  function renderReadSourcePicker() {
    const picker = document.getElementById('read-source-picker');
    if (!picker) return;

    const religions = [...new Set(READ_SOURCES.map(s => s.tradition))];
    religions.sort((a, b) => {
      const ai = READ_RELIGION_ORDER.indexOf(a), bi = READ_RELIGION_ORDER.indexOf(b);
      if (ai === -1 && bi === -1) return a.localeCompare(b);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });

    // Default to the first religion so the page never opens on an empty picker.
    if (!readReligionKey || !religions.includes(readReligionKey)) {
      readReligionKey = religions[0] || null;
    }

    picker.innerHTML = religions.map(r => {
      const activeCls = r === readReligionKey ? ' active' : '';
      return `<button class="read-btn${activeCls}" data-read-religion="${escapeHtml(r)}">${escapeHtml(r)}</button>`;
    }).join('');

    picker.querySelectorAll('[data-read-religion]').forEach(btn => {
      btn.addEventListener('click', () => {
        readReligionKey = btn.dataset.readReligion;
        readSourceKey = null;
        readBookKey = null;
        readGroupKey = null;
        renderReadSourcePicker();
      });
    });

    renderReadTextDropdown();
  }

  function renderReadTextDropdown() {
    const container = document.getElementById('read-text-dropdown');
    if (!container) return;
    if (!readReligionKey) { container.innerHTML = ''; return; }

    const texts = READ_SOURCES.filter(s => s.tradition === readReligionKey);
    if (!texts.length) { container.innerHTML = ''; return; }

    // Keep the current selection if it's still one of this religion's texts;
    // otherwise default to the first ready one (or just the first, if none are).
    if (!readSourceKey || !texts.some(t => t.key === readSourceKey)) {
      const firstReady = texts.find(t => readSourceReady(t.key));
      readSourceKey = (firstReady || texts[0]).key;
      readBookKey = null;
      readGroupKey = null;
    }

    container.innerHTML = `
      <select class="read-text-select" id="read-text-select">
        ${texts.map(t => {
          const ready = readSourceReady(t.key);
          const selected = t.key === readSourceKey ? 'selected' : '';
          return `<option value="${t.key}" ${selected} ${ready ? '' : 'disabled'}>${escapeHtml(t.label)}${ready ? '' : ' (not loaded)'}</option>`;
        }).join('')}
      </select>
    `;

    document.getElementById('read-text-select').addEventListener('change', (e) => {
      readSourceKey = e.target.value;
      readBookKey = null;
      readGroupKey = null;
      document.getElementById('read-verses').innerHTML = '';
      renderReadChapterPicker();
    });

    renderReadChapterPicker();
  }

  function renderReadChapterPicker() {
    const container = document.getElementById('read-chapter-picker');
    if (!container) return;
    if (!readSourceKey) { container.innerHTML = ''; return; }

    if (!readSourceReady(readSourceKey)) {
      container.innerHTML = `<p class="read-empty">This text hasn't finished loading yet. Check Settings → Source Texts, or try again once you're online.</p>`;
      document.getElementById('read-verses').innerHTML = '';
      return;
    }

    if (readSourceKey === 'bible') {
      renderReadBibleBookAndChapterPicker(container);
    } else {
      renderReadSimpleChapterPicker(container);
    }
  }

  function renderReadBibleBookAndChapterPicker(container) {
    const books = readBookOrder();
    if (!books.length) {
      container.innerHTML = `<p class="read-empty">No Bible books found yet.</p>`;
      return;
    }

    const bookButtons = `<div class="read-row read-chapter-row">${books.map(b => {
      const activeCls = b === readBookKey ? ' active' : '';
      return `<button class="read-btn${activeCls}" data-read-book="${escapeHtml(b)}">${escapeHtml(b)}</button>`;
    }).join('')}</div>`;

    let chapterButtons = '';
    if (readBookKey) {
      const chapters = [...new Set(
        allBibleVerses
          .map(v => parseReadRef('bible', v.ref))
          .filter(p => p && p.book === readBookKey)
          .map(p => p.chapter)
      )].sort((a, b) => a - b);
      chapterButtons = `<div class="read-row read-chapter-row" style="margin-top:10px;">${chapters.map(c => {
        const activeCls = String(c) === readGroupKey ? ' active' : '';
        return `<button class="read-btn${activeCls}" data-read-group="${c}">${c}</button>`;
      }).join('')}</div>`;
    }

    container.innerHTML = bookButtons + chapterButtons;

    container.querySelectorAll('[data-read-book]').forEach(btn => {
      btn.addEventListener('click', () => {
        readBookKey = btn.dataset.readBook;
        readGroupKey = null;
        document.getElementById('read-verses').innerHTML = '';
        renderReadChapterPicker();
      });
    });
    container.querySelectorAll('[data-read-group]').forEach(btn => {
      btn.addEventListener('click', () => {
        readGroupKey = btn.dataset.readGroup;
        renderReadChapterPicker();
        renderReadVerses();
      });
    });
  }

  function renderReadSimpleChapterPicker(container) {
    const rows = readSourceRows(readSourceKey);
    const groups = [];
    const seen = new Set();
    rows.forEach(r => {
      const parsed = parseReadRef(readSourceKey, r.ref);
      if (!parsed || seen.has(parsed.group)) return;
      seen.add(parsed.group);
      groups.push(parsed);
    });
    groups.sort((a, b) => a.chapter - b.chapter);

    if (!groups.length) {
      container.innerHTML = `<p class="read-empty">No chapters found for this text yet.</p>`;
      return;
    }

    container.innerHTML = `<div class="read-row read-chapter-row">${groups.map(g => {
      const activeCls = g.group === readGroupKey ? ' active' : '';
      return `<button class="read-btn${activeCls}" data-read-group="${escapeHtml(g.group)}">${escapeHtml(g.group)}</button>`;
    }).join('')}</div>`;

    container.querySelectorAll('[data-read-group]').forEach(btn => {
      btn.addEventListener('click', () => {
        readGroupKey = btn.dataset.readGroup;
        renderReadChapterPicker();
        renderReadVerses();
      });
    });

    if (readGroupKey && seen.has(readGroupKey)) renderReadVerses();
  }

  function renderReadVerses() {
    const container = document.getElementById('read-verses');
    if (!container || !readSourceKey || !readGroupKey) return;

    let rows;
    let heading;
    if (readSourceKey === 'bible') {
      rows = allBibleVerses
        .map(r => ({ row: r, parsed: parseReadRef('bible', r.ref) }))
        .filter(x => x.parsed && x.parsed.book === readBookKey && String(x.parsed.chapter) === readGroupKey)
        .sort((a, b) => a.parsed.verse - b.parsed.verse);
      heading = `${readBookKey} ${readGroupKey}`;
    } else {
      rows = readSourceRows(readSourceKey)
        .map(r => ({ row: r, parsed: parseReadRef(readSourceKey, r.ref) }))
        .filter(x => x.parsed && x.parsed.group === readGroupKey)
        .sort((a, b) => a.parsed.verse - b.parsed.verse);
      heading = readGroupKey;
    }

    if (!rows.length) {
      container.innerHTML = `<p class="read-empty">Nothing found in this chapter.</p>`;
      return;
    }

    container.innerHTML = `
      <p class="read-verses-heading">${escapeHtml(heading)} — ${rows.length} verse${rows.length === 1 ? '' : 's'}</p>
      ${rows.map(x => `
        <div class="bible-result-item">
          <span class="ref">${escapeHtml(x.row.ref)}</span>
          <p>"${escapeHtml(x.row.text)}"</p>
        </div>
      `).join('')}
    `;
  }

  window.addEventListener('online', () => {
    getCachedVerseCount().then(count => {
      if (count === 0) downloadFullBible();
    });
    loadAllSourceRows().then(rows => {
      const present = new Set(rows.map(r => r.sourceId));
      if (SOURCE_TEXTS.some(s => !present.has(s.id))) downloadSourceTexts();
    });
  });

  const BIBLE_RESULTS_LIMIT = 25;

  function renderBibleResults(query, bibleResult) {
    const section = document.getElementById('bible-results-section');
    const container = document.getElementById('bible-results');
    if (!query.trim() || bibleResult.total === 0) {
      section.style.display = 'none';
      container.innerHTML = '';
      return;
    }
    const matches = bibleResult.top.slice(0, BIBLE_RESULTS_LIMIT);
    section.style.display = 'block';

    const countHtml = `<p class="results-count">Results 1-${matches.length} of <strong>${bibleResult.total.toLocaleString()}</strong> for &ldquo;${escapeHtml(query)}&rdquo;</p>`;

    container.innerHTML = countHtml + matches.map(m => `
      <div class="bible-result-item">
        <a class="ref" href="${bibleLink(m.item.ref)}" target="_blank" rel="noopener noreferrer">${m.item.ref} ↗</a>
        <p>"${highlightText(escapeHtml(m.item.text), query)}"</p>
      </div>
    `).join('');
  }

  // Bible verse search is now triggered from within render() using the corrected query

  render();
  initBibleData();
  initSourceTexts();

  /* ================= App data layer (bookmarks / search log / settings) ================= */

  const APP_DB_NAME = 'tep-app-data';

  function openAppDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(APP_DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains('bookmarks')) db.createObjectStore('bookmarks', { keyPath: 'id' });
        if (!db.objectStoreNames.contains('searchLog')) {
          const s = db.createObjectStore('searchLog', { keyPath: 'auto', autoIncrement: true });
          s.createIndex('timestamp', 'timestamp');
        }
        if (!db.objectStoreNames.contains('settings')) db.createObjectStore('settings', { keyPath: 'key' });
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function entryId(entry) {
    return (entry.religion + '|' + entry.claim).toLowerCase().replace(/[^a-z0-9]+/g, '-');
  }

  async function loadBookmarkIds() {
    const db = await openAppDB();
    return new Promise((resolve) => {
      const tx = db.transaction('bookmarks', 'readonly');
      const req = tx.objectStore('bookmarks').getAllKeys();
      req.onsuccess = () => { bookmarkIds = new Set(req.result); resolve(); };
      req.onerror = () => resolve();
    });
  }

  async function toggleBookmark(entry) {
    const id = entryId(entry);
    const db = await openAppDB();
    const tx = db.transaction('bookmarks', 'readwrite');
    const store = tx.objectStore('bookmarks');
    if (bookmarkIds.has(id)) {
      store.delete(id);
      bookmarkIds.delete(id);
    } else {
      store.add({ id, religion: entry.religion, claim: entry.claim, savedAt: Date.now() });
      bookmarkIds.add(id);
    }
    tx.oncomplete = () => {
      render();
      const bookmarksPage = document.getElementById('page-bookmarks');
      if (bookmarksPage && bookmarksPage.style.display !== 'none') renderBookmarksPage();
    };
  }

  async function logSearchHit(id) {
    try {
      const db = await openAppDB();
      const tx = db.transaction('searchLog', 'readwrite');
      tx.objectStore('searchLog').add({ id, timestamp: Date.now() });
    } catch (e) { /* non-critical */ }
  }

  async function getTopSearchedPool(limit = 20) {
    const db = await openAppDB();
    const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
    return new Promise((resolve) => {
      const tx = db.transaction('searchLog', 'readonly');
      const req = tx.objectStore('searchLog').getAll();
      req.onsuccess = () => {
        const counts = {};
        (req.result || []).forEach(row => {
          if (row.timestamp >= thirtyDaysAgo) counts[row.id] = (counts[row.id] || 0) + 1;
        });
        const pool = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, limit).map(([id]) => id);
        resolve(pool);
      };
      req.onerror = () => resolve([]);
    });
  }

  async function getSetting(key) {
    const db = await openAppDB();
    return new Promise((resolve) => {
      const tx = db.transaction('settings', 'readonly');
      const req = tx.objectStore('settings').get(key);
      req.onsuccess = () => resolve(req.result ? req.result.value : null);
      req.onerror = () => resolve(null);
    });
  }

  async function setSetting(key, value) {
    const db = await openAppDB();
    const tx = db.transaction('settings', 'readwrite');
    tx.objectStore('settings').put({ key, value });
  }

  /* ================= Toast ================= */

  function showToast(message, duration = 3000) {
    const toast = document.createElement('div');
    toast.className = 'tep-toast';
    toast.textContent = message;
    document.body.appendChild(toast);
    requestAnimationFrame(() => { toast.style.opacity = '1'; toast.style.transform = 'translateX(-50%) translateY(0)'; });
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(-50%) translateY(20px)';
      setTimeout(() => toast.remove(), 300);
    }, duration);
  }

  /* ================= Share ================= */

  // Copies both a plain-text fallback and a real <a href> HTML fragment, so
  // pasting into a rich-text field (email, Slack, Docs, etc.) drops in an
  // already-clickable link instead of a bare URL that only becomes tappable
  // if the destination happens to auto-link plain text. Plain-text-only
  // destinations (SMS, notes apps) still get the plain-text form.
  async function copyShareText(text, url) {
    const plain = `${text} ${url}`;
    if (navigator.clipboard && typeof ClipboardItem !== 'undefined') {
      try {
        const html = `${escapeHtml(text)} <a href="${escapeHtml(url)}">${escapeHtml(url)}</a>`;
        const item = new ClipboardItem({
          'text/plain': new Blob([plain], { type: 'text/plain' }),
          'text/html': new Blob([html], { type: 'text/html' })
        });
        await navigator.clipboard.write([item]);
        showToast('Link copied');
        return;
      } catch (e) { /* fall through to plain-text copy below */ }
    }
    if (navigator.clipboard) {
      await navigator.clipboard.writeText(plain);
      showToast('Link copied');
    }
  }

  async function shareEntry(entry) {
    const shareData = {
      title: 'The Elijah Project',
      text: `${entry.claim} (${entry.religion}) — see how Scripture responds, in The Elijah Project.`,
      url: window.location.href
    };
    if (navigator.share) {
      try { await navigator.share(shareData); } catch (e) { /* user cancelled */ }
    } else {
      await copyShareText(shareData.text, shareData.url);
    }
  }

  async function shareApp() {
    const shareData = {
      title: 'The Elijah Project',
      text: 'The Elijah Project (TEP) — a quick reference for real conversations. See what other traditions actually teach, side by side with what Scripture says.',
      url: location.origin + location.pathname
    };
    if (navigator.share) {
      try { await navigator.share(shareData); } catch (e) { /* user cancelled */ }
    } else {
      await copyShareText(shareData.text, shareData.url);
    }
  }

  document.getElementById('share-app-home').addEventListener('click', () => shareApp());
  document.getElementById('share-app-drawer').addEventListener('click', () => { closeDrawer(); shareApp(); });

  /* ================= Drawer navigation ================= */

  const drawer = document.getElementById('drawer');
  const drawerOverlay = document.getElementById('drawer-overlay');
  const menuOpenBtn = document.getElementById('menu-open');
  const drawerCloseBtn = document.getElementById('drawer-close');
  const drawerLinks = document.querySelectorAll('.drawer-link');
  const PAGES = ['home', 'bookmarks', 'read', 'mission', 'help', 'settings', 'sources', 'contact', 'legal'];

  function openDrawer() { drawer.classList.add('open'); drawerOverlay.classList.add('open'); }
  function closeDrawer() { drawer.classList.remove('open'); drawerOverlay.classList.remove('open'); }
  menuOpenBtn.addEventListener('click', openDrawer);
  drawerCloseBtn.addEventListener('click', closeDrawer);
  drawerOverlay.addEventListener('click', closeDrawer);

  function goToPage(page) {
    PAGES.forEach(p => {
      const el = document.getElementById('page-' + p);
      if (el) el.style.display = (p === page) ? (p === 'home' ? 'block' : 'block') : 'none';
    });
    drawerLinks.forEach(l => l.classList.toggle('active', l.dataset.page === page));
    closeDrawer();
    window.scrollTo(0, 0);
    if (page === 'bookmarks') renderBookmarksPage();
    if (page === 'read') renderReadSourcePicker();
  }

  document.querySelectorAll('[data-page]').forEach(el => {
    el.addEventListener('click', () => goToPage(el.dataset.page));
  });

  /* ================= Bookmarks page ================= */

  function renderBookmarksPage() {
    const list = document.getElementById('bookmarks-list');
    const empty = document.getElementById('bookmarks-empty');
    const bookmarkedEntries = ENTRIES.filter(e => bookmarkIds.has(entryId(e)));
    list.innerHTML = '';
    empty.style.display = bookmarkedEntries.length === 0 ? 'block' : 'none';
    bookmarkedEntries.forEach(entry => {
      list.appendChild(buildEntryCard(entry));
    });
  }

  /* ================= Argument of the Day ================= */

  let aotdEntry = null;

  async function renderArgumentOfTheDay() {
    if (ENTRIES.length === 0) return;
    const today = new Date().toISOString().split('T')[0];
    const cachedDate = await getSetting('aotd-date');
    const cachedId = await getSetting('aotd-id');
    let pickId;

    if (cachedDate === today && cachedId) {
      pickId = cachedId;
    } else {
      let pool = await getTopSearchedPool(20);
      if (pool.length < 5) pool = ENTRIES.map(entryId);
      const seed = today.split('-').reduce((acc, n) => acc + parseInt(n, 10), 0);
      pickId = pool[seed % pool.length];
      await setSetting('aotd-date', today);
      await setSetting('aotd-id', pickId);
    }

    aotdEntry = ENTRIES.find(e => entryId(e) === pickId);
    if (!aotdEntry) return;
    document.getElementById('aotd-claim').textContent = aotdEntry.claim;
    document.getElementById('aotd-religion').textContent = aotdEntry.religion;
    document.getElementById('aotd-widget').style.display = 'block';
    document.getElementById('aotd-detail').innerHTML = '';
    document.getElementById('aotd-detail').style.display = 'none';
    document.getElementById('aotd-toggle').textContent = '+';
  }

  document.querySelector('#aotd-widget .aotd-top').addEventListener('click', () => {
    const detail = document.getElementById('aotd-detail');
    const toggle = document.getElementById('aotd-toggle');
    const isOpen = detail.style.display === 'block';
    if (isOpen) {
      detail.style.display = 'none';
      toggle.textContent = '+';
    } else {
      if (aotdEntry && !detail.dataset.built) {
        detail.appendChild(buildEntryCard(aotdEntry));
        detail.dataset.built = '1';
      }
      detail.style.display = 'block';
      toggle.textContent = '−';
    }
  });

  /* ================= Theme settings ================= */

  const themeSwatches = document.querySelectorAll('.theme-swatch');
  function applyTheme(theme) {
    if (theme === 'default') {
      document.documentElement.removeAttribute('data-theme');
    } else {
      document.documentElement.setAttribute('data-theme', theme);
    }
    themeSwatches.forEach(sw => sw.classList.toggle('active', sw.dataset.theme === theme));
  }
  themeSwatches.forEach(sw => {
    sw.addEventListener('click', () => {
      applyTheme(sw.dataset.theme);
      setSetting('theme', sw.dataset.theme);
    });
  });

  /* ================= Contact form ================= */

  const contactForm = document.getElementById('contact-form');
  const contactType = document.getElementById('contact-type');
  const contactDeviceField = document.getElementById('contact-device-field');

  contactType.addEventListener('change', () => {
    contactDeviceField.style.display = contactType.value === 'Bug Report' ? 'block' : 'none';
  });

  contactForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const type = contactType.value;
    const message = document.getElementById('contact-message').value.trim();
    const device = document.getElementById('contact-device').value.trim();
    const replyEmail = document.getElementById('contact-email').value.trim();

    if (!message) {
      showToast('Add a message before posting');
      return;
    }

    let body = message;
    if (type === 'Bug Report' && device) body = `Device/Browser: ${device}\n\n${message}`;
    if (replyEmail) body += `\n\nReply to: ${replyEmail}`;

    const mailto = `mailto:contact@tep-app.com?subject=${encodeURIComponent('TEP ' + type)}&body=${encodeURIComponent(body)}`;
    window.location.href = mailto;
    showToast('Opening your email app…');
    contactForm.reset();
    contactDeviceField.style.display = 'none';
  });

  /* ================= Source diagnostics buttons ================= */

  const recheckBtn = document.getElementById('recheck-sources');
  if (recheckBtn) recheckBtn.addEventListener('click', () => recheckSources());

  const copyDiagBtn = document.getElementById('copy-diag');
  if (copyDiagBtn) {
    copyDiagBtn.addEventListener('click', async () => {
      const report = buildDiagnosticReport();
      try {
        await navigator.clipboard.writeText(report);
        showToast('Diagnostic report copied');
      } catch (err) {
        console.log(report);
        showToast('Copy failed — report printed to console');
      }
    });
  }

  /* ================= Donate button ================= */

  document.getElementById('donate-btn').addEventListener('click', () => {
    showToast('Donations coming soon');
  });

  /* ================= FAQ ================= */

  const FAQ_DATA = [
    { q: "Do I need an account to use TEP?", a: "No. TEP has no login, no accounts, and no user data collection. Everything you save (bookmarks) stays on your device." },
    { q: "Does TEP work without internet?", a: "Yes. All content, including the full KJV Bible, is downloaded to your device the first time you open the app with a connection. Full search, filters, and bookmarks all work offline afterward." },
    { q: "How does content get updated?", a: "When you're back online, TEP automatically checks for updates in the background and downloads only what's changed. A small banner lets you know when new content has been added." },
    { q: "Where do the source texts come from?", a: "Original documents are pulled from public domain sources and open licenses where available. Each entry cites its source so you can verify it independently." },
    { q: "Why does TEP include claims from other religions?", a: "You can't address what you don't accurately understand. TEP presents each tradition's actual claims alongside a biblical response, rather than a strawman version of what they believe." },
    { q: "How is 'Argument of the Day' chosen?", a: "It's pulled from the most-searched topics over the past month, so it reflects what people are actually asking about, not a fixed editorial list." },
    { q: "I found an error in an entry. What do I do?", a: "Use the Report a Bug button in Contact. Include the entry name and what's incorrect, and it'll be reviewed for correction." }
  ];

  function renderFAQ() {
    const list = document.getElementById('faq-list');
    list.innerHTML = '';
    FAQ_DATA.forEach(({ q, a }) => {
      const item = document.createElement('div');
      item.className = 'faq-item';
      item.innerHTML = `<div class="faq-q">${q}<span class="plus">+</span></div><div class="faq-a">${a}</div>`;
      item.addEventListener('click', () => item.classList.toggle('open'));
      list.appendChild(item);
    });
  }

  /* ================= Init app data ================= */

  async function initAppData() {
    await loadBookmarkIds();
    render();
    const savedTheme = await getSetting('theme');
    if (savedTheme) applyTheme(savedTheme);
    renderFAQ();
    renderArgumentOfTheDay();
  }

  initAppData();
